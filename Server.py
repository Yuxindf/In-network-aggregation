import collections
import numpy as np
import socket
from threading import Timer
import random
import time as t
from Packet import Packet, tmp_pkt

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
        self.result_from_proxy = collections.OrderedDict()
        self.clients_wait_fin = collections.OrderedDict()

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

    # Receive basic information from client and send to proxy
    def client_basic_info(self, info, address):
        # Receive basic information from client
        connection_trails_count = 0
        client_seq = info.seq
        cal_type = info.msg.split(delimiter)[1]
        packet_number = info.msg.split(delimiter)[2]
        self.clients[info.client_id]["job id"] = info.job_id
        self.clients[info.client_id]["cal type"] = info.msg.split(delimiter)[1]
        self.clients[info.client_id]["packet number"] = int(info.msg.split(delimiter)[2])
        if info.msg.count(delimiter) == 3:
            self.clients[info.client_id]["weight"] = float(info.msg.split(delimiter)[3])
        # Send basic information to proxy
        # msg will include operation type, client address, client seq and size
        msg = "client info" + delimiter + str(address) + delimiter + str(client_seq) + delimiter + str(info.job_id) +\
              delimiter + str(info.client_id) + delimiter + cal_type + delimiter + packet_number
        self.clients[info.client_id]["server seq"] = self.seq
        self.clients[info.client_id]["client seq"] = info.seq
        self.send_packet(msg, proxy_address)

    # do some calculations
    def obtain_aggregation_result(self, pkt):
        # Obtain information
        client_id = int(pkt.msg.split(delimiter)[1])
        data = float(pkt.msg.split(delimiter)[2])
        job_id = self.clients[client_id]["job id"]
        cal_type = self.clients[client_id]["cal type"]
        client_address = self.clients[client_id]["address"]
        if job_id not in self.result_from_proxy.keys():
            # Initialize the dictionary
            self.result_from_proxy[job_id] = {"cal type": cal_type, "client id": [client_id],
                                              "client address": [client_address], "data": [], "time": t.time()}
        else:
            # Add new client
            self.result_from_proxy[job_id]["client id"].append(client_id)
            self.result_from_proxy[job_id]["client address"].append(client_address)
        if "weight" in self.clients[client_id].keys():
            weight = self.clients[client_id]["weight"]
            self.result_from_proxy[job_id]["data"].append((data, weight))
        else:
            self.result_from_proxy[job_id]["data"] = np.append(self.result_from_proxy[job_id]["data"], data)

    def calculate(self, key):
        cal_type = self.result_from_proxy[key]["cal type"]
        if cal_type == "maximum":
            ans = self.result_from_proxy[key]["data"].max()
        elif cal_type == "minimum":
            ans = self.result_from_proxy[key]["data"].min()
        elif cal_type == "average":
            sum = 0
            number = len(self.result_from_proxy[key]["data"])
            for i in self.result_from_proxy[key]["data"]:
                data = i[0]
                weight = i[1]
                sum += data * weight
            ans = sum / number
        for id in self.clients_wait_fin.keys():
            self.send_packet(ans, self.clients[id]["address"])
            print("Sending result to client (%s, %s)" % self.clients[id]["address"])
            fin_ack_number = self.clients_wait_fin[id]["fin ack number"]
            msg = "FIN" + delimiter + str(1) + delimiter + "ACK" + delimiter + str(1) \
                  + delimiter + "ack number" + delimiter + str(fin_ack_number)
            self.clients_wait_fin[id]["server seq"] = self.seq
            self.send_packet(msg, self.clients[id]["address"])
        self.result_from_proxy.pop(key)

    def send_final_result(self):
        for key in self.result_from_proxy.keys():
            if len(self.result_from_proxy[key]["client id"]) == 2:
                self.calculate(key)

    def monitor_job(self):
        for key in self.result_from_proxy.keys():
            if t.time() - self.result_from_proxy[key]["time"] > 5:
                self.calculate(key)
            else:
                break

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        print("\nWaiting to receive message")
        timer = RepeatingTimer(10.0, self.monitor_job)
        timer.start()
        # Timer(2, self.ru, ()).start()
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
                self.clients[decoded_pkt.client_id] = {"address": address, "server seq": self.seq - 1}

            # Third handshake
            elif decoded_pkt.msg.split(delimiter)[0] == "SYN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) ==1:
                last_seq = int(decoded_pkt.msg.split(delimiter)[3])
                if self.clients[decoded_pkt.client_id]["server seq"] + 1 == last_seq:
                    self.clients[decoded_pkt.client_id]["state"] = "connected"
                    print("Connected with client (%s, %s)" % address)

            # Receive client basic information
            elif "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)

            elif "proxy ack" in decoded_pkt.msg:
                client_id = int(decoded_pkt.msg.split(delimiter)[1])
                if int(decoded_pkt.msg.split(delimiter)[2]) == self.clients[client_id]["server seq"] + 1:
                    # Send Ack to client
                    self.send_packet(self.clients[client_id]["client seq"] + 1, self.clients[client_id]["address"])

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(decoded_pkt.seq + 1)
                self.send_packet(msg, address)
                self.clients_wait_fin[decoded_pkt.client_id] = {"fin ack number": decoded_pkt.seq + 1}

            elif "aggregation result" in decoded_pkt.msg:
                print("Receive one aggregation result of job %s from proxy" % decoded_pkt.msg.split(delimiter)[1])
                self.obtain_aggregation_result(decoded_pkt)
                self.send_final_result()

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                for i in self.clients_wait_fin.keys():
                    if self.clients[i]["address"] == address:
                        if self.clients_wait_fin[i]["server seq"] + 1 == int(decoded_pkt.msg.split(delimiter)[3]):
                            self.clients.pop(i)
                            self.clients_wait_fin.pop(i)
                            print("Disconnect with client (%s, %s)" % address)
                            break


if __name__ == '__main__':
    server = Server()
    server.run()
