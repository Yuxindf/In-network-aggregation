import collections
import numpy as np
import json
import socket
from threading import Timer
import random
import time as t
from Packet import Packet

serverAddress = "localhost"
serverPort = 10000
proxyAddress = "localhost"
proxyPort = 6001
proxy_address = (proxyAddress, proxyPort)
timeout = 100

data_list = []
seq = 1

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200
data_list = []
client_list = []


class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)


class Server:
    def __init__(self):
        self.seq = random.randrange(1024)  # The current sequence number
        self.offset = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((serverAddress, serverPort))  # Bind the socket to the port

        self.clients = collections.OrderedDict()
        # self.clients["2"] = {"address": ["127.0.0.1", 62128], "server seq": 531, "state": "connected", "job id": 2,
        #        "cal type": "minimum", "packet number": 2120, "client seq": 85}  # For test
        self.result_from_proxy = collections.OrderedDict()
        # self.result_from_proxy["2"] = {"cal type": "minimum", "client id": [1,3], "client address": [["127.0.0.1", 62534]], "data": [24.0],
        #        "time": 1615381549.3457372} # For test
        self.clients_wait_fin = collections.OrderedDict()
        # self.clients_wait_fin["2"] = {"fin ack number": 2435, "server seq": 602}  # For test

        # Backup data in files
        self.clients_file = "./server/clients.txt"
        self.result_from_proxy_file = "./server/proxy_result.txt"
        self.clients_wait_fin_file = "./server/clients_wait_fin.txt"

    def send_packet(self, msg, address):
        self.offset += 1
        pkt = Packet(0, 0, self.seq, self.offset, msg, 0)
        self.seq += 1
        pkt.encode_seq()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            print("Fail to send packet")
        return pkt

    def load_file(self, file):
        with open(file, "r") as f:
            lines = f.read()
        f.close()
        lines = json.loads(lines)
        return lines

    # Backup data in the file
    def backup(self, file, data):
        with open(file, "w") as f:
            store = json.dumps(data)
            f.write(store)
        f.close()

    # Receive basic information from client and send to proxy
    def client_basic_info(self, info, address):
        # Receive basic information from client
        connection_trails_count = 0
        client_seq = info.seq
        cal_type = info.msg.split(delimiter)[1]
        packet_number = info.msg.split(delimiter)[2]
        self.clients[str(info.client_id)]["job id"] = info.job_id
        self.clients[str(info.client_id)]["cal type"] = info.msg.split(delimiter)[1]
        self.clients[str(info.client_id)]["packet number"] = int(info.msg.split(delimiter)[2])
        if info.msg.count(delimiter) == 3:
            self.clients[str(info.client_id)]["weight"] = float(info.msg.split(delimiter)[3])
        # Send basic information to proxy
        # msg will include operation type, client address, client seq and size
        msg = "client info" + delimiter + str(address) + delimiter + str(client_seq) + delimiter + str(info.job_id) +\
              delimiter + str(info.client_id) + delimiter + cal_type + delimiter + packet_number
        self.clients[str(info.client_id)]["server seq"] = self.seq
        self.clients[str(info.client_id)]["client seq"] = info.seq
        # Store clients' information in a file
        self.backup(self.clients_file, self.clients)
        self.send_packet(msg, proxy_address)

    # do some calculations
    def obtain_aggregation_result(self, pkt):
        # Obtain information
        client_id = int(pkt.msg.split(delimiter)[1])
        data = float(pkt.msg.split(delimiter)[2])
        job_id = self.clients[str(client_id)]["job id"]
        cal_type = self.clients[str(client_id)]["cal type"]
        client_address = self.clients[str(client_id)]["address"]
        if str(job_id) not in self.result_from_proxy.keys():
            # Initialize the dictionary
            self.result_from_proxy[str(job_id)] = {"cal type": cal_type, "client id": [client_id],
                                              "client address": [client_address], "data": [], "time": t.time()}
        else:
            # Add new client
            self.result_from_proxy[str(job_id)]["client id"].append(client_id)
            self.result_from_proxy[str(job_id)]["client address"].append(client_address)
        # Weighted average
        if "weight" in self.clients[str(client_id)].keys():
            weight = self.clients[str(client_id)]["weight"]
            self.result_from_proxy[str(job_id)]["data"].append((data, weight))
        # Minimum and Maximum
        else:
            self.result_from_proxy[str(job_id)]["data"].append(data)
        # Backup results from proxy in a file
        self.backup(self.result_from_proxy_file, self.result_from_proxy)

    def calculate(self, key):
        cal_type = self.result_from_proxy[key]["cal type"]
        if cal_type == "average":
            sum = 0
            number = len(self.result_from_proxy[key]["data"])
            for i in self.result_from_proxy[key]["data"]:
                data = i[0]
                weight = i[1]
                sum += data * weight
            ans = sum / number
        else:
            data = []
            for i in self.result_from_proxy[key]["data"]:
                data = np.append(data, i)
            if cal_type == "maximum":
                ans = data.max()
            elif cal_type == "minimum":
                ans = data.min()

        self.result_from_proxy.pop(key)
        # Update results from proxy in the file
        self.backup(self.result_from_proxy_file, self.result_from_proxy)

        for id in self.clients_wait_fin.keys():
            self.send_packet(ans, self.clients[id]["address"])
            print("Sending result to client (%s, %s)" % self.clients[id]["address"])
            fin_ack_number = self.clients_wait_fin[id]["fin ack number"]
            msg = "FIN" + delimiter + str(1) + delimiter + "ACK" + delimiter + str(1) \
                  + delimiter + "ack number" + delimiter + str(fin_ack_number)
            self.clients_wait_fin[id]["server seq"] = self.seq
            # Update clients that wait in fin in the file
            self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
            self.send_packet(msg, self.clients[id]["address"])

    def send_final_result(self):
        for key in self.result_from_proxy.keys():
            if len(set(self.result_from_proxy[key]["client id"])) == 2:  # For test, will change by reading from a file
                self.calculate(key)

    def monitor_job(self):
        for key in self.result_from_proxy.keys():
            if t.time() - self.result_from_proxy[key]["time"] > 1000000:  # For test, will change
                self.calculate(key)
            else:
                break

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        # Load data
        self.clients = self.load_file(self.clients_file)
        self.result_from_proxy = self.load_file(self.result_from_proxy_file)
        self.clients_wait_fin = self.load_file(self.clients_wait_fin_file)
        self.clients = collections.OrderedDict(self.clients)
        self.result_from_proxy = collections.OrderedDict(self.result_from_proxy)
        self.clients_wait_fin = collections.OrderedDict(self.clients_wait_fin)
        print("\nWaiting to receive message")
        timer = RepeatingTimer(10.0, self.monitor_job)  # For test, will change
        timer.start()
        # Listening for requests indefinitely
        while True:
            # Start - Connection initiation
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            # First and Second Handshake
            if decoded_pkt.msg.split(delimiter)[0] == "SYN" and str(1) in decoded_pkt.msg:
                msg = "ack number" + delimiter + str(decoded_pkt.seq + 1) + delimiter + "SYN" + delimiter + str(1) + \
                      delimiter + "ACK" + delimiter + str(1)
                self.send_packet(msg, address)
                self.clients[str(decoded_pkt.client_id)] = {"address": address, "server seq": self.seq - 1}

            # Third handshake
            elif decoded_pkt.msg.split(delimiter)[0] == "SYN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) ==1:
                last_seq = int(decoded_pkt.msg.split(delimiter)[3])
                if self.clients[str(decoded_pkt.client_id)]["server seq"] + 1 == last_seq:
                    self.clients[str(decoded_pkt.client_id)]["state"] = "connected"
                    print("Connected with client (%s, %s)" % address)

            # Receive client basic information
            elif "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)

            elif "proxy ack" in decoded_pkt.msg:
                client_id = int(decoded_pkt.msg.split(delimiter)[1])
                if int(decoded_pkt.msg.split(delimiter)[2]) == self.clients[str(client_id)]["server seq"] + 1:
                    # Send Ack to client
                    self.send_packet(self.clients[str(client_id)]["client seq"] + 1, self.clients[str(client_id)]["address"])

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(decoded_pkt.seq + 1)
                self.send_packet(msg, address)
                self.clients_wait_fin[str(decoded_pkt.client_id)] = {"fin ack number": decoded_pkt.seq + 1}
                # Backup clients that wait in fin in a file
                self.backup(self.clients_wait_fin_file, self.clients_wait_fin)

            elif "aggregation result" in decoded_pkt.msg:
                print("Receive one aggregation result of job %s from proxy" % decoded_pkt.msg.split(delimiter)[1])
                self.obtain_aggregation_result(decoded_pkt)
                self.send_final_result()

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                for key in self.clients_wait_fin.keys():
                    if self.clients[str(key)]["address"] == address:
                        print(address)
                        if self.clients_wait_fin[key]["server seq"] + 1 == int(decoded_pkt.msg.split(delimiter)[3]):
                            self.clients.pop(key)
                            # Update connected clients in the file
                            self.backup(self.clients_file, self.clients)
                            self.clients_wait_fin.pop(key)
                            # Update clients that wait in fin in the file
                            self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
                            print("Disconnect with client (%s, %s)" % address)
                            break


if __name__ == '__main__':
    server = Server()
    server.run()
