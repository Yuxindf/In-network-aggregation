# Server

import collections
import numpy as np
import json
import socket
import random
import time as t
from Packet import Packet

serverAddress = "localhost"
serverPort = 10000
proxyAddress = "localhost"
proxyPort = 6001
# Test using Mininet
# serverAddress = '10.0.0.1'
# serverPort = 25139
# proxyAddress = '10.0.0.2'
# proxyPort = 25137

proxy_address = (proxyAddress, proxyPort)

# Delimiter
delimiter = "|*|*|"
size = 1000
client_list = []

# Data type flags
IS_SYN = 1
IS_INFO = 2 # Client send basic information to server
IS_DATA = 3
IS_ACK = 4
IS_FIN = 5


class Server:
    def __init__(self):
        self.seq = random.randrange(1024)  # The current sequence number
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((serverAddress, serverPort))  # Bind the socket to the port

        self.rwnd = 500000

        # Backup data in files
        self.tasks_file = "./server/tasks.txt"  # Backup tasks
        self.clients_file = "./server/clients.txt"  # Backup clients
        self.wait_fin_file = "./server/wait_fin.txt"  # Backup wait fin
        self.data_file = "./server/data.txt"  # Backup data
        self.cache_file = "./proxy/cache.txt"  # Backup cache

        self.clients = collections.OrderedDict()  # Store clients that has sent information to server
        self.wait_fin = collections.OrderedDict()  # Store clients that finish sending packets
        self.data = collections.OrderedDict()  # Store sequence number of packets that has been arrived
        self.cache = collections.OrderedDict()  # Store data that has been arrived
        # Load tasks from file
        self.tasks = self.load_file(self.tasks_file)
        self.tasks = collections.OrderedDict(self.tasks)
    
    # Send packet
    def send_packet(self, flag, seq, msg, address, index):
        if seq == -1:
            pkt = Packet(flag, 0, 0, self.seq, index, msg, 0)
            self.seq += 1
        else:
            pkt = Packet(flag, 0, 0, seq, index, msg, 0)
        pkt.encode_buf()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            print("Fail to send packet")
        return pkt
    
    # Load file from file system
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
        weight = -1
        # Receive basic information from client
        client_seq = info.seq
        packet_number = info.msg.split(delimiter)[1].split(" ")[0]
        self.clients[str(info.client_id)]["job id"] = info.job_id
        self.clients[str(info.client_id)]["cal type"] = self.tasks[str(info.job_id)]["cal type"]
        self.clients[str(info.client_id)]["packet number"] = int(info.msg.split(delimiter)[1])
        if info.msg.count(delimiter) == 2:
            self.clients[str(info.client_id)]["weight"] = float(info.msg.split(delimiter)[2])
            weight = self.clients[str(info.client_id)]["weight"]
        # Send basic information to proxy
        # msg will include operation type, client address, client seq and size
        msg = str(address) + delimiter + str(client_seq) + delimiter + str(info.job_id) + delimiter + \
              str(info.client_id) + delimiter + packet_number + delimiter + str(weight)
        self.clients[str(info.client_id)]["address"] = address
        self.clients[str(info.client_id)]["server seq"] = self.seq
        self.clients[str(info.client_id)]["client seq"] = info.seq
        self.clients[str(info.client_id)]["next seq"] = client_seq + 1
        self.clients[str(info.client_id)]["relocate seq"] = False
        self.clients[str(info.client_id)]["correct seq"] = False
        self.clients[str(info.client_id)]["state"] = 0
        if str(info.job_id) not in self.cache:
            self.cache[str(info.job_id)] = {}
        return msg

    # Receive data from client or proxy and send ACK back
    def recv_data(self, pkt, address):
        client_id = str(pkt.client_id)
        # Receive data
        if self.rwnd > 0 and pkt.msg.split("  ")[0] != "data":
            # Initialize
            if client_id not in self.data.keys():
                self.data[client_id] = []
            data = pkt.msg.split(delimiter)[1]
            if pkt.seq not in self.data[client_id]:
                # Receive data from Client
                if pkt.client_id != 0:
                    # Weighted average
                    if self.clients[client_id]["cal type"] == "average":
                        data = data.split(" ")[0]
                        number = float(data[1:-1].split(",")[0])
                        weight = float(data[1:-1].split(",")[1])
                        data = (number, weight)
                    # Maximum or Minimum
                    else:
                        data = float(data)

                # Receive data from Proxy
                else:
                    data = float(pkt.msg.split(delimiter)[1])
                    number = int(pkt.msg.split(delimiter)[2])
                    if self.tasks[str(pkt.job_id)]["cal type"] == "average":
                        data = (data, number)
                    else:
                        data = data
                wait_aggregation = [pkt.job_id, pkt.client_id, pkt.index, data]
                self.data[client_id].append(pkt.seq)
                # Do aggregation
                self.aggregate(wait_aggregation)

            # Normal situation
            if pkt.seq == self.clients[client_id]["next seq"]:
                self.clients[client_id]["next seq"] += 1
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
                self.clients[client_id]["correct seq"] = True

            # Duplicate ack
            else:
                self.clients[client_id]["correct seq"] = False
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
                self.clients[client_id]["relocate seq"] = True
            # After receiving packet that has three duplicate ACK, find the next packet that wants to receive
            if self.clients[client_id]["correct seq"] and self.clients[client_id]["relocate seq"]:
                self.clients[client_id]["relocate seq"] = False
                i = self.clients[client_id]["next seq"]
                while True:
                    if i not in self.data[client_id]:
                        self.clients[client_id]["next seq"] = i
                        print("Relocate: Client %s, now seq %s" %(client_id,i))
                        break
                    i += 1
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            # Send ACK to client or proxy
            self.send_packet(IS_ACK, self.seq, msg, address, -1)
        # Empty packet
        elif pkt.msg.split("  ")[0] == "data":
            msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            self.send_packet(IS_ACK, self.seq, msg, address, -1)

    # Do aggregation when receiving a data from proxy or client
    def aggregate(self, w):
        index = str(w[2])
        job_id = str(w[0])
        if index in self.cache[job_id].keys():
            data_number = self.cache[job_id][index]
            # Weighted average
            if self.tasks[job_id]["cal type"] == "average":
                if w[1] != 0:  # From client
                    weight = self.clients[str(w[1])]["weight"]
                    ans = (data_number[0] * data_number[1] + w[3][0] * w[3][1] * weight) / (data_number[1] + 1)
                else:  # From proxy
                    ans = (data_number[0] * data_number[1] + w[3][0] * w[3][1]) / (data_number[1] + w[3][1])
            # Maximum
            elif self.tasks[job_id]["cal type"] == "maximum":
                ans = max(self.cache[job_id][index][1], w[3])
            # Minimum
            else:
                ans = min(self.cache[job_id][index][1], w[3])
            # Handle answer
            self.cache[job_id][index] = [ans, data_number[1] + 1]
        else:
            if self.tasks[job_id]["cal type"] == "average":
                if w[1] != 0:  # From client
                    weight = self.clients[str(w[1])]["weight"]
                    ans = w[3][0] * w[3][1] * weight
                else:  # From proxy
                    ans = w[3][0]
            else:
                ans = w[3]
            self.cache[job_id][index] = [ans, 1]
            self.rwnd -= 1

    # Do aggregation for all clients of a job
    def final_aggregation(self, key):
        job_id = str(key)
        self.tasks[job_id]["flag"] = 1
        cal_type = self.tasks[job_id]["cal type"]
        ans = 0
        # Average
        if cal_type == "average":
            total = 0
            number = len(self.cache[job_id])
            for i in self.cache[job_id].keys():
                item = self.cache[job_id][i]
                data = item[0]
                num = item[1]
                total += data * num
            ans = total / number
        # Minimum or Maximum
        else:
            data = []
            for i in self.cache[job_id].keys():
                data = np.append(self.cache[job_id][i][0], data)
            if cal_type == "maximum":
                ans = np.max(data)
            elif cal_type == "minimum":
                ans = np.min(data)

        for i in self.wait_fin[job_id].keys():
            msg = str(ans)
            address = self.clients[i]["address"]
            self.send_packet(IS_DATA, -1, msg, address, -1)
            self.clients[str(i)]["state"] = "sending result"
            self.clients[i]["server seq"] = self.seq - 1

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        print("\nWaiting to receive message")

        # Listening for requests indefinitely
        while True:
            # Start - Connection initiation
            try:
                self.sock.settimeout(3600)
                recv, address = self.sock.recvfrom(size)
            except socket.timeout:
                # Backup files
                self.backup(self.clients_file, self.clients)
                self.backup(self.wait_fin_file, self.wait_fin)
                self.backup(self.data_file, self.data)
                self.backup(self.cache_file, self.cache)
                self.backup(self.tasks_file, self.tasks)
            # Decode packet
            decoded_pkt = Packet(0, 0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_buf()
            client_id = decoded_pkt.client_id
            job_id = decoded_pkt.job_id
            dmsg = decoded_pkt.msg

            # Three-way handshake
            if decoded_pkt.flag == IS_SYN:
                # First and Second Handshake
                if dmsg.split(delimiter)[0] == "SYN" and str(1) in dmsg:
                    msg = "ack number" + delimiter + str(decoded_pkt.seq + 1) + delimiter + "SYN" + delimiter + str(1) + \
                          delimiter + "ACK" + delimiter + str(1)
                    self.send_packet(IS_SYN, self.seq, msg, address, -1)
                    self.clients[str(client_id)] = {"client address": address, "server seq": self.seq}
                # Third handshake
                elif dmsg.split(delimiter)[0] == "SYN ACK" and int(dmsg.split(delimiter)[1]) ==1:
                    next_seq = int(dmsg.split(delimiter)[3])
                    if self.clients[str(client_id)]["server seq"] + 1 == next_seq:
                        self.clients[str(client_id)]["state"] = "connected"
                        print("Connected with client (%s, %s)" % address)
                        # Send the task to this client, including job id and calculation type
                        for i in self.tasks:
                            if client_id in self.tasks[i]["clients"]:
                                cal_type = self.tasks[i]["cal type"]
                                self.send_packet(IS_DATA, -1, i + delimiter + cal_type, address, -1)
                                break

            # Receive client basic information
            elif decoded_pkt.flag == IS_INFO:
                if "client info" in dmsg:
                    msg = self.receive_client_info(decoded_pkt, address)
                    self.clients[str(client_id)]["to server"] = False
                    self.send_packet(IS_INFO, -1, msg, proxy_address, -1)
                # Clients connect with server directly
                elif "Connect directly: info" in dmsg:
                    self.receive_client_info(decoded_pkt, address)
                    self.clients[str(client_id)]["to server"] = True
                    # Send Ack to client
                    self.send_packet(IS_ACK, self.seq, self.clients[str(client_id)]["client seq"] + 1, address, -1)

            # Receive proxy ack for basic information
            # Or Third wave: after sending result to client and receive ack
            elif decoded_pkt.flag == IS_ACK:
                # Receive proxy ack for basic information
                if "proxy ack" in dmsg:
                    c_id = dmsg.split(delimiter)[1]
                    if str(0) not in self.clients.keys():
                        self.clients[str(0)] = {"client address": proxy_address,
                                                "next seq": decoded_pkt.seq + 1, "relocate seq": False, "correct seq": False}
                    if int(dmsg.split(delimiter)[2]) == self.clients[c_id]["server seq"] + 1:
                        # Send Ack to client
                        self.send_packet(IS_ACK, self.seq, self.clients[c_id]["client seq"] + 1,
                                         self.clients[c_id]["client address"], -1)
                # Third wave: after sending result to client and receive ack
                elif "Result ACK" in dmsg:
                    server_seq = int(dmsg.split(delimiter)[1])
                    for i in self.clients:
                        if "state" in self.clients[i] and self.clients[i]["state"] == "sending result" \
                                and self.clients[i]["server seq"] + 1 == server_seq:
                            client_seq = self.clients[i]["client seq"]
                            msg = "FIN" + delimiter + str(1) + delimiter + "ACK" + delimiter + str(1) + delimiter + \
                                  "ack number" + delimiter + str(client_seq)
                            self.clients[i]["server_seq"] = self.seq
                            self.send_packet(IS_FIN, self.seq, msg, address, -1)
                            self.clients[i]["state"] = "FIN"

            # Receive data packet of clients from proxy or clients
            elif decoded_pkt.flag == IS_DATA:
                self.recv_data(decoded_pkt, address)

            # Disconnect with client
            elif decoded_pkt.flag == IS_FIN:
                # Second handshake (FIN)
                if dmsg.split(delimiter)[0] == "FIN" and int(dmsg.split(delimiter)[1]) == 1:
                    print("FIN from client %s" % client_id)
                    if str(client_id) in self.clients:
                        self.clients[str(client_id)]["client seq"] = decoded_pkt.seq
                    msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(decoded_pkt.seq + 1)
                    self.send_packet(IS_FIN, self.seq, msg, address, -1)
                    if str(job_id) not in self.wait_fin:
                        self.wait_fin[str(job_id)] = {}
                    # When data is sent to server directly
                    if str(client_id) not in self.wait_fin[str(job_id)] and self.clients[str(client_id)]["to server"]:
                        self.wait_fin[str(job_id)][str(client_id)] = {"fin ack number": decoded_pkt.seq + 1,
                                                                      "receive all": True}
                    # When data is sent to proxy
                    elif str(client_id) not in self.wait_fin[str(job_id)] \
                            and not self.clients[str(client_id)]["to server"]:
                        self.wait_fin[str(job_id)][str(client_id)] = {"fin ack number": decoded_pkt.seq + 1,
                                                                      "receive all": False}
                    print("Receive all data from client %s" % client_id)
                    # Whether the server receives all the data of all the packets of a job
                    receive_all = True
                    clients = []
                    for i in self.wait_fin[str(job_id)].keys():
                        clients.append(int(i))
                        if not self.wait_fin[str(job_id)][i]["receive all"]:
                            receive_all = False
                            break

                    if receive_all and set(self.tasks[str(job_id)]["clients"]) == set(clients):
                        print("Job id %s: receive all" % job_id)
                        self.final_aggregation(job_id)

                # Fourth handshake (FIN)
                elif "FIN ACK" in dmsg:
                    server_seq = int(dmsg.split(delimiter)[3])
                    for i in list(self.clients):
                        if "state" in self.clients[i] and self.clients[i]["state"] == "FIN" \
                                and self.clients[i]["server seq"] + 2 == server_seq and int(dmsg.split(delimiter)[1]) == 1:
                            # Record clients that send final FIN
                            self.tasks[str(job_id)]["arrived"].append(int(i))
                            self.clients.pop(i)
                            self.wait_fin[str(job_id)].pop(str(client_id))
                            if self.wait_fin[str(job_id)] == {}:
                                self.wait_fin.pop(str(job_id))
                            # If all the clients of a task send final FIN, the task will be deleted
                            if set(self.tasks[str(job_id)]["arrived"]) == set(self.tasks[str(job_id)]["clients"]):
                                self.tasks.pop(str(job_id))
                                print("Finish task %s" % job_id)
                            print("Disconnect with client %s" % i)

                # Fin of a client from proxy
                # to tell the server that the packets of the client has all been received by server
                elif address[1] == proxy_address[1]:
                    msg = str(decoded_pkt.seq + 1) + delimiter + str(self.rwnd)
                    self.send_packet(IS_FIN, self.seq, msg, proxy_address, -1)
                    if str(job_id) not in self.wait_fin:
                        self.wait_fin[str(job_id)] = {}
                    if str(client_id) not in self.wait_fin[str(job_id)]:
                        self.wait_fin[str(job_id)][str(client_id)] = {"receive all": True}
                    else:
                        self.wait_fin[str(job_id)][str(client_id)]["receive all"] = True
                    receive_all = True
                    clients = []
                    for i in self.wait_fin[str(job_id)].keys():
                        clients.append(int(i))
                        if not self.wait_fin[str(job_id)][i]["receive all"]:
                            receive_all = False
                            break
                    if receive_all and set(self.tasks[str(job_id)]["clients"]) == set(clients):
                        print("Job id %s: receive all" % job_id)
                        self.final_aggregation(job_id)


if __name__ == '__main__':
    server = Server()
    server.run()
