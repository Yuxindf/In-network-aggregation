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
client_list = []

# Data type flags
IS_SYN = 1
IS_INFO = 2
IS_DATA = 3
IS_ACK = 4
IS_FIN = 5


class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)


class Server:
    def __init__(self):
        self.time = t.time()  # Test, will delete
        self.seq = random.randrange(1024)  # The current sequence number
        self.offset = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((serverAddress, serverPort))  # Bind the socket to the port

        self.rwnd = 10000

        # Backup data in files
        self.tasks_file = "./server/tasks.txt"
        self.clients_file = "./server/clients.txt"
        self.wait_fin_file = "./server/wait_fin.txt"
        self.data_file = "./server/data.txt"
        self.cache_file = "./proxy/cache.txt"

        # self.clients = self.load_file(self.clients_file)
        # self.wait_fin = self.load_file(self.wait_fin_file)
        # self.data = self.load_file(self.data_file)
        # self.cache = self.load_file(self.cache_file)

        self.clients = collections.OrderedDict()#self.clients)
        self.wait_fin = collections.OrderedDict()#self.wait_fin)
        self.data = collections.OrderedDict()#self.data)
        self.cache = collections.OrderedDict()

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
        connection_trails_count = 0
        client_seq = info.seq
        print(info.msg)
        cal_type = info.msg.split(delimiter)[1]
        packet_number = info.msg.split(delimiter)[2].split(" ")[0]
        self.clients[str(info.client_id)]["job id"] = info.job_id
        self.clients[str(info.client_id)]["cal type"] = info.msg.split(delimiter)[1]
        self.clients[str(info.client_id)]["packet number"] = int(info.msg.split(delimiter)[2])
        if info.msg.count(delimiter) == 3:
            self.clients[str(info.client_id)]["weight"] = float(info.msg.split(delimiter)[3])
            weight = self.clients[str(info.client_id)]["weight"]
        # Send basic information to proxy
        # msg will include operation type, client address, client seq and size
        msg = str(address) + delimiter + str(client_seq) + delimiter + str(info.job_id) + delimiter + \
              str(info.client_id) + delimiter + cal_type + delimiter + packet_number + delimiter + str(weight)
        self.clients[str(info.client_id)]["address"] = address
        self.clients[str(info.client_id)]["server seq"] = self.seq
        self.clients[str(info.client_id)]["client seq"] = info.seq
        self.clients[str(info.client_id)]["next seq"] = client_seq + 1
        self.clients[str(info.client_id)]["relocate seq"] = False
        self.clients[str(info.client_id)]["correct seq"] = False
        self.clients[str(info.client_id)]["state"] = 0
        # Store clients' information in a file
        self.backup(self.clients_file, self.clients)
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
                # Backup data from clients in a file
                self.backup(self.data_file, self.data)
            data = pkt.msg.split(delimiter)[1]
            if pkt.seq not in self.data[client_id]:
                # Receive data from Client
                if pkt.client_id != 0:
                    # Weighted average
                    if self.clients[client_id]["cal type"] == "average":
                        data = data.split("  ")[0]
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
                # Update data from clients in the file
                self.backup(self.data_file, self.data)
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
            self.backup(self.clients_file, self.clients)
            if self.clients[client_id]["correct seq"] and self.clients[client_id]["relocate seq"]:
                self.clients[client_id]["relocate seq"] = False
                # for i in range(self.clients[client_id]["next seq"],
                #                self.clients[client_id]["next seq"] + self.clients[client_id]["packet number"]):
                i = self.clients[client_id]["next seq"]
                while True:
                    if i not in self.data[client_id]:
                        self.clients[client_id]["next seq"] = i
                        print("now %s" %i)
                        break
                    i += 1
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            # Send ACK to client or proxy
            self.send_packet(IS_ACK, self.seq, msg, address, -1)
            self.backup(self.clients_file, self.clients)
        # Empty packet
        elif pkt.msg.split("  ")[0] == "data":
            msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            self.send_packet(IS_ACK, self.seq, msg, address, -1)

    # Do aggregation when receiving a data
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
            self.backup(self.cache_file, self.cache)
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
            self.backup(self.cache_file, self.cache)
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
                print(data)
                ans = np.min(data)

        for i in self.wait_fin[job_id].keys():
            msg = str(ans)
            address = self.clients[i]["address"]
            print(address)
            self.send_packet(IS_DATA, -1, msg, address, -1)
            self.clients[str(i)]["state"] = "sending result"
            self.clients[i]["server seq"] = self.seq - 1
            self.backup(self.clients_file, self.clients)

                #
        #
        # for id in self.aggregation_result[key]["client id"]:
        #     id = str(id)
        #     self.send_packet(IS_DATA, ans, self.clients[id]["client address"], -1)  # No ack here
        #     print("Sending result to client with id %s" % id)
        #     while True:
        #         if id in self.clients_wait_fin.keys():
        #             break
        #     fin_ack_number = self.clients_wait_fin[id]["fin ack number"]
        #     msg = "FIN" + delimiter + str(1) + delimiter + "ACK" + delimiter + str(1) \
        #           + delimiter + "ack number" + delimiter + str(fin_ack_number)
        #     self.clients_wait_fin[id]["server seq"] = self.seq
        #     # Update clients that wait in fin in the file
        #     self.backup(self.clients_wait_fin_file, self.clients_wait_fin)
        #     self.send_packet(msg, self.clients[id]["client address"])
        #
        # self.aggregation_result.pop(key)
        # # Update results from proxy in the file
        # self.backup(self.aggregation_result_file, self.aggregation_result)
        #
        # self.tasks.pop(key)
        # # Update tasks
        # self.backup(self.tasks_file, self.tasks)

    # Ignore clients that do not send data for a long time
    def monitor_job(self):
        # if "wait fin" in self.clients and self.clients["wait fin"] != {}:
        #     print(self.clients["wait fin"])
        #     for i in self.clients["wait fin"]:
        #         # Ensure each client of the same job is iterated only once
        #         if self.clients[i]["state"] != "wait":
        #             continue
        #         job_id = str(self.clients["wait fin"][i]["job id"])
        #         if set(self.jobs[job_id]) != set(self.tasks[job_id]["clients"]):
        #             if t.time() - self.clients[j]["time"] > self.timeout:
        #                 for j in self.jobs[job_id]:
        #                     self.clients[str(j)]["state"] = "wait"
        #                 for k in self.cache[job_id].items():
        #                     self.wait_send[str(self.count)] = {"job id": int(k), "index": int(k[0])}
        #                     self.count += 1
        #                 self.send_to_server()
        #                 break
        #         if t.time() - self.clients[i]["time"] > 2 * self.timeout:
        #             for j in self.jobs[job_id]:
        #                 self.clients[str(j)]["state"] = "wait"
        #             for k in self.cache[job_id].items():
        #                 self.wait_send[str(self.count)] = {"job id": int(k), "index": int(k[0])}
        #                 self.count += 1
        #             self.send_to_server()
        #             break

        # self.send_final_result()
        return 0

    # clear cache
    def clear_cache(self):
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

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        # Load data
        self.tasks = self.load_file(self.tasks_file)

        self.tasks = {"1": {"clients":[1,2,5], "cal type":"average", "flag":0}}#collections.OrderedDict(self.tasks)

        print("\nWaiting to receive message")
        timer = RepeatingTimer(10.0, self.monitor_job)  # For test, will change
        timer.start()
        # Listening for requests indefinitely
        while True:
            if self.rwnd == 0:
                self.clear_cache()
            # Start - Connection initiation
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_buf()
            client_id = decoded_pkt.client_id
            job_id = decoded_pkt.job_id
            dmsg = decoded_pkt.msg

            # Three handshakes
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
                        # Send task to this client, including job id and calculation type
                        for i in self.tasks:
                            if client_id in self.tasks[i]["clients"]:
                                print(self.tasks)
                                cal_type = self.tasks[i]["cal type"]
                                self.send_packet(IS_DATA, -1, i + delimiter + cal_type, address, -1)

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
                            self.backup(self.clients_file, self.clients)

            # Receive data packet of clients from proxy or clients
            elif decoded_pkt.flag == IS_DATA:
                self.recv_data(decoded_pkt, address)

            # Disconnect with client
            elif decoded_pkt.flag == IS_FIN:
                # Second wave
                if dmsg.split(delimiter)[0] == "FIN" and int(dmsg.split(delimiter)[1]) == 1:
                    print("FIN from client %s" % client_id)
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
                    receive_all = True
                    clients = []
                    for i in self.wait_fin[str(job_id)].keys():
                        clients.append(int(i))
                        if not self.wait_fin[str(job_id)][i]["receive all"]:
                            receive_all = False
                            break
                    # print(receive_all)
                    # print(self.tasks[str(job_id)]["clients"])
                    # print(clients)
                    if receive_all and set(self.tasks[str(job_id)]["clients"]) == set(clients):
                        self.final_aggregation(job_id)
                    # Backup clients that wait in fin in a file
                    self.backup(self.wait_fin_file, self.wait_fin)

                # Fourth wave
                elif "FIN ACK" in dmsg:
                    print("%s %s %s" % (self.clients[str(client_id)]["server seq"],int(dmsg.split(delimiter)[3]), client_id))
                    server_seq = int(dmsg.split(delimiter)[3])
                    for i in list(self.clients):
                        if "state" in self.clients[i] and self.clients[i]["state"] == "FIN" \
                                and self.clients[i]["server seq"] + 2 == server_seq and int(dmsg.split(delimiter)[1]) == 1:

                            self.clients.pop(i)
                            self.backup(self.clients_file, self.clients)
                            self.wait_fin[str(job_id)].pop(str(client_id))
                            if self.wait_fin[str(job_id)] == {}:
                                self.wait_fin.pop(str(job_id))
                            self.backup(self.wait_fin_file, self.wait_fin)
                            print("Disconnect with client %s" % i)

                # Fin of a client from proxy
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
                        print("111")
                        self.final_aggregation(job_id)


if __name__ == '__main__':
    server = Server()
    server.run()
