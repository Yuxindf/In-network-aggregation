# udp server
import collections
import socket
import threading
from threading import Timer
import time as t
import random
import json
from Packet import Packet

proxyAddress = '127.0.0.1'
serverAddress = '127.0.0.1'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port
server_address = (serverAddress, server_port)

wait_ack_list = []
result_list = []
client_info = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200

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

class Proxy:
    def __init__(self):
        # Proxy initial state
        self.seq = random.randrange(1024)  # The current sequence number
        self.count = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((proxyAddress, proxy_port))

        self.tasks = collections.OrderedDict()
        self.clients = collections.OrderedDict()
        self.jobs = collections.OrderedDict()
        self.data = collections.OrderedDict()
        self.cache = collections.OrderedDict()
        self.wait_send = collections.OrderedDict()

        # Backup data in files
        self.tasks_file = "./proxy/tasks.txt"
        self.clients_file = "./proxy/clients.txt"
        self.jobs_file = "./proxy/jobs.txt"
        self.data_file = "./proxy/data.txt"
        self.cache_file = "./proxy/cache.txt"

        # Load data
        self.clients = self.load_file(self.clients_file)
        self.data = self.load_file(self.data_file)
        # self.cache = self.load_file(self.cache_file)
        self.clients = collections.OrderedDict()#self.clients)
        self.data = collections.OrderedDict()#self.data)
        self.cache = collections.OrderedDict()#self.cache)

        # Client basic information

        # Congestion control
        self.cwnd = 3  # initial congestion window size
        self.ssthresh = 1000  #
        self.packet_index = 0
        self.number_in_flight = 0
        self.packets_in_flight = collections.OrderedDict()
        self.packets_retransmit = collections.OrderedDict()

        self.srtt = -1  # Smooth round-trip timeout
        self.devrtt = 0  # calculate the devision of srtt and real rtt
        self.rto = 10  # Retransmission timeout
        self.time_last_lost = 0
        self.last_retransmit = -1

        self.is_in_thread = False
        self.timeout = 5

        # flow control
        self.rwnd = 1000
        self.server_rwnd = 1000

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

    def send_packet(self, flag, job_id, client_id, seq, msg, address, index):
        if seq == -1:
            self.seq += 1
            pkt = Packet(flag, job_id, client_id, self.seq, index, msg, 0)
        else:
            pkt = Packet(flag, job_id, client_id, seq, index, msg, 0)
        pkt.encode_buf()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            print("Fail to send packet")
        return pkt

    # Receive basic information of client from server and send ACK to server and client
    def client_basic_info(self, info, address):
        msg = info.msg  # Client Basic Information
        # Obtain calculation type and packet number
        client_seq = int(msg.split(delimiter)[1])
        job_id = int(msg.split(delimiter)[2])
        client_id = int(msg.split(delimiter)[3])
        cal_type = msg.split(delimiter)[4]
        packet_number = int(msg.split(delimiter)[5])
        weight = float(msg.split(delimiter)[6])
        # Send Ack to Server
        ack = "proxy ack" + delimiter + str(client_id) + delimiter + str(info.seq + 1)
        self.send_packet(IS_ACK, job_id, 0, self.seq, ack, address, -1)
        # obtain client address
        c = msg.split(delimiter)[0]
        client_address = str(c[1:-1].split(", ")[0][1:-1])
        client_port = c[1:-1].split(", ")[1]
        client_address = (client_address, int(client_port))
        print("Receive basic information of a client %s " % c)
        print("Calculation type is %s \nNumber of Packets is %s" % (cal_type, packet_number))
        if "wait fin" not in self.clients:
            self.clients["wait fin"] = {}
        if "fin" not in self.clients:
            self.clients["fin"] = {}
        self.clients[str(client_id)] = {"client address": client_address, "job id": job_id, "cal type": cal_type,
                                        "packet number": packet_number, "weight": weight,
                                        "next seq": client_seq + 1, "relocate seq": False, "correct seq": False,
                                        "time": -1, "state": 0 }
        # Backup clients' basic information in a file
        self.backup(self.clients_file, self.clients)
        if str(job_id) not in self.jobs.keys():
            self.jobs[str(job_id)] = []
        self.jobs[str(job_id)].append(client_id)
        self.backup(self.jobs_file, self.jobs)
        if str(job_id) not in self.cache:
            self.cache[str(job_id)] = {}

    # Receive data from client and send ACK back
    def recv_data(self, pkt, address):
        client_id = str(pkt.client_id)
        job_id = str(pkt.job_id)
        # Send Ack to Client
        # print(address)
        print("rwnd %s" % self.rwnd)
        # Receive data
        if self.rwnd > 0 and pkt.msg.split("  ")[0] != "data":
            # Initialize
            if client_id not in self.data.keys():
                self.data[client_id] = []
                # Backup data from clients in a file
                self.backup(self.data_file, self.data)
            data = pkt.msg.split(delimiter)[1]
            if pkt.seq not in self.data[client_id]:
                # Weighted average
                if self.clients[client_id]["cal type"] == "average":
                    data = data.split("  ")[0]
                    number = float(data[1:-1].split(",")[0])
                    weight = float(data[1:-1].split(",")[1])
                    data = (number, weight)
                # Maximum or Minimum
                else:
                    data = float(data)
                wait_aggregation = [pkt.job_id, int(client_id), pkt.index, data]
                # Do aggregation
                self.aggregate(wait_aggregation)
                self.data[client_id].append(pkt.seq)
                # Update data from clients in the file
                self.backup(self.data_file, self.data)

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
                for i in range(self.clients[client_id]["next seq"],
                               self.clients[client_id]["next seq"] + self.clients[client_id]["packet number"]):
                    if i not in self.data[client_id]:
                        self.clients[client_id]["next seq"] = i
                        print("address %s now %s" %(address, i))
                        break
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            # Send ACK to client
            # print(str(self.clients[client_id]["next seq"]))
            self.send_packet(IS_ACK, pkt.job_id, 0, self.seq, msg, address, -1)
            self.backup(self.clients_file, self.clients)
        # Empty packet
        elif pkt.msg.split("  ")[0] == "data":
            msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            self.send_packet(IS_ACK, pkt.job_id, 0, self.seq, msg, address, -1)

        # Whether receive all the packets from a client
        if client_id not in self.clients["wait fin"] \
                and self.clients[client_id]["packet number"] == len(self.data[client_id]):
            self.clients["wait fin"][client_id] = {"job id": pkt.job_id, "time": t.time()}
            if set(int(i) for i in self.clients["wait fin"]) == set(self.tasks[job_id]["clients"]) \
                    and not self.is_in_thread:
                thread = threading.Thread(target=self.send_fin)
                thread.start()

    def monitor_job(self):
        if "wait fin" in self.clients and self.clients["wait fin"] != {}:
            # print(self.clients["wait fin"])
            for i in self.clients["wait fin"]:
                # Ensure each client of the same job is iterated only once
                if self.clients[i]["state"] != "wait":
                    continue
                job_id = str(self.clients["wait fin"][i]["job id"])
                if set(self.jobs[job_id]) != set(self.tasks[job_id]["clients"]):
                    if t.time() - self.clients[i]["time"] > self.timeout:
                        for j in self.jobs[job_id]:
                            self.clients[str(j)]["state"] = "wait"
                        for k in self.cache[job_id].items():
                            self.wait_send[str(self.count)] = {"job id": int(k), "index": int(k[0])}
                            self.count += 1
                        self.send_to_server()
                        break
                if t.time() - self.clients[i]["time"] > 2 * self.timeout:
                    for j in self.jobs[job_id]:
                        self.clients[str(j)]["state"] = "wait"
                    for k in self.cache[job_id].items():
                        self.wait_send[str(self.count)] = {"job id": int(k), "index": int(k[0])}
                        self.count += 1
                    self.send_to_server()
                    break

    # self.cache{"job_id":{"index":[data,number,time,client_id],...},...}
    # self.data{"client id":[],...}
    # self.index{"data":[[job, client, index, data],...]}
    # self.jobs{"job_id":[],...}
    def aggregate(self, a):
        index = str(a[2])
        job_id = str(a[0])
        if index in self.cache[job_id].keys():
            data_number = self.cache[job_id][index]
            # Weighted average
            if self.clients[str(a[1])]["cal type"] == "average":
                weight = self.clients[str(a[1])]["weight"]
                ans = (data_number[0] * data_number[1] + a[3][0] * a[3][1] * weight) / (data_number[1] + 1)
            # Maximum
            elif self.clients[a[1]]["cal type"] == "maximum":
                ans = max(self.cache[job_id][index][1], a[3])
            # Minimum
            else:
                ans = min(self.cache[job_id][index][1], a[3])
            # Handle answer
            time = self.cache[job_id][index][2]
            self.cache[job_id][index] = [ans, data_number[1] + 1, time]
            self.backup(self.cache_file, self.cache)
        else:
            if self.clients[str(a[1])]["cal type"] == "average":
                weight = self.clients[str(a[1])]["weight"]
                ans = a[3][0] * a[3][1] * weight
            else:
                ans = a[3]
            self.cache[job_id][index] = [ans, 1, t.time()]
            self.backup(self.cache_file, self.cache)
            self.rwnd -= 1

        clients = self.jobs[job_id]
        number = len(clients)
        print(self.tasks)
        print(self.jobs)
        if set(self.tasks[job_id]["clients"]) == set(self.jobs[job_id]):
            data_number = self.cache[job_id][index]
            for i in clients:
                if a[2] > self.clients[str(i)]["packet number"]:
                    number -= 1
            if (data_number[1]) == number:
                self.wait_send[str(self.count)] = {"job id": a[0], "index": a[2]}
                self.count += 1

    # Send aggregation result to server
    def send_to_server(self):
        self.number_in_flight = min(max(0, self.number_in_flight), len(self.packets_in_flight))
        # Send packets to server
        # print("cwnd %s rwnd %s flight %s" %(self.cwnd,self.rwnd,self.number_in_flight))
        if min(self.cwnd, self.server_rwnd) > self.number_in_flight:
            while min(self.cwnd, self.server_rwnd) > self.number_in_flight:
                # Retransmit: Three duplicate acks.
                for key in self.packets_in_flight.keys():
                    if self.packets_in_flight[key]["duplicate acks"] >= 3:
                        msg = self.packets_in_flight[key]["msg"]
                        self.number_in_flight += 1
                        self.send_packet(IS_DATA, self.packets_in_flight[key]["job id"], 0,
                                         int(key), msg, server_address,
                                         self.packets_in_flight[key]["index"])
                        self.packets_in_flight[key]["duplicate acks"] = 0
                        self.packets_in_flight[key]["time"] = t.time()
                        if key != self.last_retransmit and t.time() - self.time_last_lost > self.srtt:
                            self.cwnd /= 2
                            self.ssthresh /= 2
                            self.last_retransmit = key
                            self.time_last_lost = t.time()
                    break
                if min(self.cwnd, self.server_rwnd) <= self.number_in_flight:
                    break

                # Retransmit: timeout
                if self.packets_retransmit != {}:
                    for key in list(self.packets_retransmit.keys()):
                        if key in self.packets_in_flight:
                            msg = self.packets_in_flight[key]["msg"]
                            self.number_in_flight += 1
                            self.send_packet(IS_DATA, self.packets_in_flight[key]["job id"], 0,
                                             int(key), msg, server_address,
                                             self.packets_in_flight[key]["index"])
                            self.packets_in_flight[key]["duplicate acks"] = 0
                            self.packets_in_flight[key]["time"] = t.time()
                            self.packets_retransmit.pop(key)
                            print("Retransmit: packet index %s" % self.packets_in_flight[key]["index"])
                            if min(self.cwnd, self.server_rwnd) <= self.number_in_flight:
                                break
                if min(self.cwnd, self.server_rwnd) <= self.number_in_flight:
                    break
                # Send data that are in cache to server
                self.send_new_data()
                break

    # Send data
    def send_new_data(self):
        for i in list(self.wait_send):
            job_id = self.wait_send[i]["job id"]
            index = self.wait_send[i]["index"]
            data = self.cache[str(job_id)][str(index)][0]
            number = self.cache[str(job_id)][str(index)][1]
            msg = "data" + delimiter + str(data) + delimiter + str(number)
            self.packets_in_flight[str(self.seq)] = {"index": index, "msg": msg, "time": t.time(),
                                                     "duplicate acks": 0, "job id": job_id}
            self.send_packet(IS_DATA, job_id, 0, -1, msg, server_address, index)
            self.number_in_flight += 1
            self.cache[str(job_id)].pop(str(index))
            self.rwnd += 1
            self.wait_send.pop(i)
            self.backup(self.cache_file, self.cache)
            if min(self.cwnd, self.server_rwnd) <= self.number_in_flight:
                break

    # 带权平均，packet header可以带一个权重，默认为1。server那里可知，做到全局平均。
    # Clear cache
    # self.cache{"job_id":{"index":[data,number,time],...},...}
    # self.data{"client id":[],...}
    # self.index{"data":[[job, client, index, data],...]}
    # self.jobs{"job_id":[],...}
    def clear_cache(self):
        self.number_in_flight = min(max(0, self.number_in_flight), len(self.packets_in_flight))
        # print("clear cwnd %s server rwnd %s in flight %s" % (self.cwnd, self.server_rwnd, self.number_in_flight))
        # Send packets
        if min(self.cwnd, self.server_rwnd) > self.number_in_flight:
            while min(self.cwnd, self.server_rwnd) > self.number_in_flight:
                self.send_new_data()
                min_time = 10000000000
                job_id = 0
                for i in self.cache.keys():
                    for j in self.cache[i].keys():
                        item = self.cache[i][j]
                        min_time = min(min_time, item[2])
                        if min_time == item[2]:
                            job_id = i
                        break
                for i in self.cache[job_id].keys():
                    item = self.cache[job_id][i]
                    msg = "data" + delimiter + str(item[0]) + delimiter + str(item[1])
                    if item[1] == 1:
                        self.send_packet(IS_DATA, int(job_id), 0, -1, msg, server_address, int(i))
                    else:
                        self.send_packet(IS_DATA, int(job_id), 0, -1, msg, server_address, int(i))
                    self.cache[job_id].pop(i)
                    self.rwnd += 1
                    self.packets_in_flight[self.seq] = {"index": i, "msg": msg, "time": t.time(), "duplicate acks": 0}
                    break
                break

    # Receive ack from server
    def receive_ack(self, pkt, address):
        self.number_in_flight -= 1
        # Slow start
        if self.cwnd < self.ssthresh:
            self.cwnd += 1
        # Congestion control
        else:
            self.cwnd += 1.0 / self.cwnd
        # Receive ack of data
        next_seq = int(pkt.msg.split(delimiter)[0])

        self.server_rwnd = int(pkt.msg.split(delimiter)[1])
        if self.number_in_flight > -1:
            # Remove data that are ensured to be received
            for i in list(self.packets_in_flight.keys()):
                if int(i) <= next_seq - 1:
                    send_time = self.packets_in_flight[i]["time"]
                    self.packets_in_flight.pop(i)
                    # Calculate SRTT and RTO
                    receive_time = t.time()
                    rtt = receive_time - send_time
                    # srtt, Jacobson / Karels algorithm
                    if self.srtt < 0:
                        # First time setting srtt
                        self.srtt = rtt
                        self.devrtt = rtt / 2
                        self.rto = self.srtt + max(0.010, 4 * self.devrtt)
                    else:
                        alpha = 0.125
                        beta = 0.25
                        self.devrtt = (1 - beta) * self.devrtt + beta * abs(rtt - self.srtt)
                        self.srtt = (1 - alpha) * self.srtt + alpha * rtt
                        self.rto = self.srtt + max(0.010, 4 * self.devrtt)
                        self.rto = max(self.rto, 1)  # Always round up RTO.
                        self.rto = min(self.rto, 60)  # Maximum value 60 seconds.
                else:
                    # Record duplicate ack
                    if int(i) == next_seq:
                        self.packets_in_flight[i]["duplicate acks"] += 1
                    # Situation of losing packets
                    if t.time() - self.packets_in_flight[i]["time"] >= self.rto:
                        self.packets_retransmit[i] = ""
                        self.packets_in_flight.pop(i)
                        if t.time() - self.time_last_lost > self.srtt:
                            self.ssthresh = 1 / 2 * self.cwnd
                            self.cwnd = 3
                            self.time_last_lost = t.time()
                            continue
                    else:
                        break

    # When the server receives all packets of a client from the proxy, the proxy informs the server
    def send_fin(self):
        self.is_in_thread = True
        t.sleep(0.5)
        while True:
            for i in list(self.clients["wait fin"]):
                if i not in self.clients["fin"]:
                    flag = 1
                    job_id = self.clients["wait fin"][i]["job id"]
                    client_id = int(i)
                    packet_number = self.clients[i]["packet number"]
                    for j in list(self.cache[str(job_id)].keys()):
                        if int(j) < packet_number:
                            # print("111")
                            # print(self.cache)
                            flag = 0
                    if flag:
                        for j in self.packets_in_flight.keys():
                            if self.packets_in_flight[j]["index"] < packet_number:
                                # print("222")
                                # print(self.packets_in_flight)
                                flag = 0
                    if flag:
                        seq = self.seq
                        print("Telling server receives all packets from client with id %s..." % client_id)
                        self.send_packet(IS_FIN, job_id, client_id, seq, "", server_address, -1)
                        self.clients["fin"][i] = {"seq": seq, "time": t.time(), "job id": job_id}
            if self.clients["wait fin"] == {} and self.clients["fin"] == {}:
                self.is_in_thread = False
                break
            else:
                t.sleep(0.5)

    def run(self):
        print("Proxy start up on %s port %s\n" % (proxyAddress, proxy_port))
        self.tasks = {"1": {"clients": [1,2,5], "cal type": "min", "flag": 0}}  # collections.OrderedDict(self.tasks)
        timer = RepeatingTimer(5.0, self.monitor_job)  # For test, will change
        timer.start()
        while True:
            if self.rwnd < 0:
                self.clear_cache()
            recv = ""
            try:
                self.sock.settimeout(5)
                recv, address = self.sock.recvfrom(size)
            except socket.timeout:
                self.monitor_job()
            if recv == "":
                continue
            decoded_pkt = Packet(0, 0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_buf()
            if decoded_pkt.flag == IS_INFO:
                self.client_basic_info(decoded_pkt, address)
            elif decoded_pkt.flag == IS_DATA:
                self.recv_data(decoded_pkt, address)
                self.send_to_server()
            elif decoded_pkt.flag == IS_ACK:
                self.receive_ack(decoded_pkt, address)
                self.send_to_server()
            elif decoded_pkt.flag == IS_FIN:
                if self.clients["fin"] != {}:
                    for i in list(self.clients["fin"]):
                        if self.clients["fin"][i]["seq"] + 1 == int(decoded_pkt.msg.split(delimiter)[0]):
                            self.clients["wait fin"].pop(i)
                            self.clients["fin"].pop(i)
                            self.clients.pop(i)
                            self.backup(self.clients_file, self.clients)
                            print("all")
                            self.server_rwnd = int(decoded_pkt.msg.split(delimiter)[1])
                            self.data.pop(i)
                            self.backup(self.data_file, self.data)


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

