import collections
import numpy as np
import json
import socket
from threading import Timer
import random
import time as t
import threading
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

        self.rwnd = 1000

        self.tasks = collections.OrderedDict()
        self.clients = collections.OrderedDict()
        self.result_from_proxy = collections.OrderedDict()
        self.clients_wait_fin = collections.OrderedDict()
        # Clients connect with server directly
        self.data = collections.OrderedDict()
        self.calculate = collections.OrderedDict()

        # Backup data in files
        self.tasks_file = "./server/tasks.txt"
        self.clients_file = "./server/clients.txt"
        self.result_from_proxy_file = "./server/proxy_result.txt"
        self.clients_wait_fin_file = "./server/clients_wait_fin.txt"
        self.data_file = "./server/data.txt"
        self.calculate_file = "./server/calculate.txt"

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
    def receive_client_info(self, info, address):
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
        self.clients[str(info.client_id)]["next seq"] = client_seq + 1
        self.clients[str(info.client_id)]["relocate seq"] = False
        self.clients[str(info.client_id)]["correct seq"] = False
        # Store clients' information in a file
        self.backup(self.clients_file, self.clients)
        return msg

    # do some calculations
    def obtain_aggregation_result(self, pkt):
        # Obtain information
        client_id = int(pkt.msg.split(delimiter)[1])
        data = float(pkt.msg.split(delimiter)[2])
        job_id = self.clients[str(client_id)]["job id"]
        cal_type = self.clients[str(client_id)]["cal type"]
        client_address = self.clients[str(client_id)]["client address"]
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

    # Calculate data that are aggregated
    def do_calculation(self, key):
        cal_type = self.result_from_proxy[key]["cal type"]
        ans = 0
        if cal_type == "average":
            total = 0
            number = len(self.result_from_proxy[key]["data"])
            for i in self.result_from_proxy[key]["data"]:
                data = i[0]
                weight = i[1]
                total += data * weight
            ans = total / number
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
            self.send_packet(ans, self.clients[id]["client address"])
            print("Sending result to client with id %s" % id)
            fin_ack_number = self.clients_wait_fin[id]["fin ack number"]
            msg = "FIN" + delimiter + str(1) + delimiter + "ACK" + delimiter + str(1) \
                  + delimiter + "ack number" + delimiter + str(fin_ack_number)
            self.clients_wait_fin[id]["server seq"] = self.seq
            # Update clients that wait in fin in the file
            self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
            self.send_packet(msg, self.clients[id]["client address"])

    # Send final result to client
    def send_final_result(self):
        for key in self.result_from_proxy.keys():
            if key in list(self.tasks.keys()):
                if set(self.tasks[key]["clients"]) == set(self.result_from_proxy[key]["client id"]):
                    print("qwe")
                    self.do_calculation(key)
                    self.tasks.pop(key)
                    # Update tasks
                    self.backup(self.tasks_file, self.tasks)

    # Ignore clients that do not send data for a long time
    def monitor_job(self):
        self.tasks = self.load_file(self.tasks_file)
        self.tasks = collections.OrderedDict(self.tasks)
        for key in self.result_from_proxy.keys():
            if t.time() - self.result_from_proxy[key]["time"] > 1000000:  # For test, will change
                self.do_calculation(key)
            else:
                break

    # Clients connect with server directly
    # Receive data from client and send ACK back
    def recv_data(self, pkt, address):
        client_id = str(pkt.client_id)
        # Send Ack to Client
        # Receive data
        if self.rwnd >0 and "finish" not in pkt.msg and pkt.msg.split("  ")[0] != "data":
            # Initialize
            if client_id not in self.data.keys():
                self.data[client_id] = {"data": [], "seq": [], "result": [0, 0]}
                # Backup data from clients in a file
                self.backup(self.data_file, self.data)
            data = pkt.msg.split(delimiter)[1]
            if pkt.seq not in self.data[client_id]["seq"]:
                # Weighted average
                if self.clients[client_id]["cal type"] == "average":
                    number = float(data[1:-1].split(",")[0])
                    weight = float(data[1:-1].split(",")[1])
                    data = (number, weight)
                # Maximum or Minimum
                else:
                    data = float(data)
                self.data[client_id]["data"].append(data)
                self.data[client_id]["seq"].append(pkt.seq)
                # Modify the value of receive window size
                self.rwnd -= 1
                # Update data from clients in the file
                self.backup(self.data_file, self.data)

            # Normal situation
            if pkt.seq == self.clients[client_id]["next seq"]:
                self.clients[client_id]["next seq"] += 1
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
                self.clients[client_id]["correct seq"] = True
                if "finish" in pkt.msg:
                    print("Receive all packets from client (%s, %s)" % address)
                    if client_id in self.data.keys():
                        self.calculate[client_id] = {}
                        # Backup data that wait to be calculated in a file
                        self.backup(self.calculate_file, self.calculate)
            # Duplicate ack
            else:
                self.clients[client_id]["correct seq"] = False
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
                self.clients[client_id]["relocate seq"] = True
            self.backup(self.clients_file, self.clients)
            if self.clients[client_id]["correct seq"] and self.clients[client_id]["relocate seq"]:
                self.clients[client_id]["relocate seq"] = False
                for i in range(self.clients[client_id]["next seq"],
                               self.clients[client_id]["next seq"] + self.clients[client_id]["packet number"]):
                    if i not in self.data[client_id]["seq"]:
                        self.clients[client_id]["next seq"] = i
                        print("now %s" %i)
                        break
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            self.send_packet(msg, address)
            self.backup(self.clients_file, self.clients)
            print(msg)
            print("Receive packet %s from Client %s, sending ack..." % (pkt.seq, address))

        elif pkt.msg.split("  ")[0] == "data":
            print("123321")
            msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            self.send_packet(msg, address)

        elif self.rwnd > 0 and "finish" in pkt.msg:
            if pkt.seq == self.clients[client_id]["next seq"]:
                self.clients[client_id]["next seq"] += 1
            msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            print("Receive all packets from client (%s, %s)" % address)
            self.send_packet(msg, address)
            if client_id in self.data.keys():
                self.calculate[client_id] = {}
                # Backup data that wait to be calculated in a file
                self.backup(self.calculate_file, self.calculate)
            # print(self.data)

    # do some calculations for data that are sent by client
    def aggregate(self):
        for key in list(self.calculate.keys()):
            # Check if there is data that is not be aggregated
            if self.data[key]["data"]:
                # Weighted average
                if self.clients[key]["cal type"] == "average":
                    total = 0
                    for i in self.data[key]["data"]:
                        total += i[0] * i[1]
                    result = self.data[key]["result"]
                    self.data[key]["result"][0] = (result[0] * result[1] + total) / (result[1] + len(self.data[key]["data"]))
                    # Weighted average
                else:
                    data = []
                    for i in self.data[key]["data"]:
                        data = np.append(data, i)
                    if self.clients[key]["cal type"] == "maximum":
                        ans = data.max()
                        if self.data[key]["result"][1] != 0:
                            self.data[key]["result"][0] = max(ans, self.data[key]["result"][0])
                        else:
                            self.data[key]["result"][0] = ans
                    elif self.clients[key]["cal type"] == "minimum":
                        ans = data.min()
                        if self.data[key]["result"][1] != 0:
                            self.data[key]["result"][0] = min(ans, self.data[key]["result"][0])
                        else:
                            self.data[key]["result"][0] = ans
                # Update the number of data that has been aggregated
                self.data[key]["result"][1] += len(self.data[key]["data"])
                # Modify the value of receive window size
                self.rwnd += len(self.data[key]["data"])
                self.data[key]["data"] = []
                self.backup(self.data_file, self.data)
                self.calculate.pop(key)
                # Update data that wait to be calculated in the file
                self.backup(self.calculate_file, self.calculate)
            # Check if the client has sent all the packets
            if self.clients[key]["packet number"] == self.data[key]["result"][1]:
                print("The result of client %s is %s" % (self.clients[key]["client address"], ans))
                # Do "aggregation" like proxy
                msg = "aggregation result" + delimiter + str(key) + delimiter + str(ans)
                tmp_pkt = Packet(0, 0, 0, 0, msg, 0)
                tmp_pkt.encode_seq()
                pkt = Packet(0, 0, 0, 0, 0, tmp_pkt.buf)
                pkt.decode_seq()
                self.obtain_aggregation_result(pkt)

                self.data.pop(key)
                # Update data
                self.backup(self.data_file, self.data)
                self.calculate.pop(key)
                # Update data that wait to be calculated in the file
                self.backup(self.calculate_file, self.calculate)

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        # Load data
        self.tasks = self.load_file(self.tasks_file)
        self.clients = self.load_file(self.clients_file)
        self.result_from_proxy = self.load_file(self.result_from_proxy_file)
        self.clients_wait_fin = self.load_file(self.clients_wait_fin_file)
        self.tasks = {"1": {"clients":[1,3]}, "2": {"clients":[1,2]}, "3": {"clients":[1,2,3]}}#collections.OrderedDict(self.tasks)
        self.clients = collections.OrderedDict(self.clients)
        self.result_from_proxy = collections.OrderedDict(self.result_from_proxy)
        self.clients_wait_fin = collections.OrderedDict(self.clients_wait_fin)
        print("\nWaiting to receive message")
        timer = RepeatingTimer(10.0, self.monitor_job)  # For test, will change
        timer.start()
        # Listening for requests indefinitely
        while True:
            if self.rwnd == 0:
                for i in self.data.keys():
                    if self.data[i]["data"]:
                        self.calculate[i] = {}
                self.aggregate()
            # Start - Connection initiation
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            # First and Second Handshake
            if decoded_pkt.msg.split(delimiter)[0] == "SYN" and str(1) in decoded_pkt.msg:
                msg = "ack number" + delimiter + str(decoded_pkt.seq + 1) + delimiter + "SYN" + delimiter + str(1) + \
                      delimiter + "ACK" + delimiter + str(1)
                self.send_packet(msg, address)
                self.clients[str(decoded_pkt.client_id)] = {"client address": address, "server seq": self.seq - 1}

            # Third handshake
            elif decoded_pkt.msg.split(delimiter)[0] == "SYN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) ==1:
                last_seq = int(decoded_pkt.msg.split(delimiter)[3])
                if self.clients[str(decoded_pkt.client_id)]["server seq"] + 1 == last_seq:
                    self.clients[str(decoded_pkt.client_id)]["state"] = "connected"
                    print("Connected with client (%s, %s)" % address)

            # Receive client basic information
            elif decoded_pkt.msg.split(delimiter)[0] == "client info":
                msg = self.receive_client_info(decoded_pkt, address)
                self.send_packet(msg, proxy_address)

            # Receive proxy ack
            elif "proxy ack" in decoded_pkt.msg:
                client_id = int(decoded_pkt.msg.split(delimiter)[1])
                if int(decoded_pkt.msg.split(delimiter)[2]) == self.clients[str(client_id)]["server seq"] + 1:
                    # Send Ack to client
                    self.send_packet(self.clients[str(client_id)]["client seq"] + 1, self.clients[str(client_id)]["client address"])

            elif "client info directly" in decoded_pkt.msg:
                self.receive_client_info(decoded_pkt, address)
                self.send_packet(decoded_pkt.seq + 1, address)

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(decoded_pkt.seq + 1)
                self.send_packet(msg, address)
                self.clients_wait_fin[str(decoded_pkt.client_id)] = {"fin ack number": decoded_pkt.seq + 1}
                # Backup clients that wait in fin in a file
                self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
                self.send_final_result()

            elif "aggregation result" in decoded_pkt.msg:
                print("Receive one aggregation result of client %s from proxy" % decoded_pkt.msg.split(delimiter)[1])
                self.obtain_aggregation_result(decoded_pkt)
                msg = "server ack" + delimiter + str(decoded_pkt.seq + 1) + delimiter + str(decoded_pkt.msg.split(delimiter)[1])
                self.send_packet(msg, address)
                self.send_final_result()

            elif decoded_pkt.msg.split(delimiter)[0] == "FIN ACK" and int(decoded_pkt.msg.split(delimiter)[1]) == 1:
                for key in self.clients_wait_fin.keys():
                    if self.clients[str(key)]["client address"] == address:
                        if self.clients_wait_fin[key]["server seq"] + 1 == int(decoded_pkt.msg.split(delimiter)[3]):
                            self.clients.pop(key)
                            # Update connected clients in the file
                            self.backup(self.clients_file, self.clients)
                            self.clients_wait_fin.pop(key)
                            # Update clients that wait in fin in the file
                            self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
                            print("Disconnect with client (%s, %s)" % address)
                            break

            # Clients connect with server directly
            elif "Connect directly: info" in decoded_pkt.msg:
                self.receive_client_info(decoded_pkt, address)
                client_id = decoded_pkt.client_id
                # Send Ack to client
                self.send_packet(self.clients[str(client_id)]["client seq"] + 1, address)
            elif "data" in decoded_pkt.msg:
                self.recv_data(decoded_pkt, address)
                if self.calculate != {}:
                    t = threading.Thread(target=self.aggregate)
                    t.start()


if __name__ == '__main__':
    server = Server()
    server.run()
