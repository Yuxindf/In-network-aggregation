# udp proxy
import collections
import socket
import threading
import time as t
import random
import json
from Packet import Packet

proxyAddress = '127.0.0.1'
serverAddress = '127.0.0.1'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port
# Test using Mininet
# serverAddress = '10.0.0.1'
# server_port = 25139
# proxyAddress = '10.0.0.2'
# proxy_port = 25137

server_address = (serverAddress, server_port)


# Delimiter
delimiter = "|*|*|"
size = 1000

# Data type flags
IS_SYN = 1
IS_INFO = 2
IS_DATA = 3
IS_ACK = 4
IS_FIN = 5

class Proxy:
    def __init__(self):
        # Proxy initial state
        self.seq = random.randrange(1024)  # The current sequence number
        self.count = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((proxyAddress, proxy_port))

        self.clients = collections.OrderedDict()  # Store clients that has sent basic information and will go through proxy
        self.jobs = collections.OrderedDict()  # Store jobs that client has arrived at proxy
        self.data = collections.OrderedDict()  # Store sequence number of packets of each client
        self.cache = collections.OrderedDict()  # Store data that has been arrived
        # Store info of index that has aggregated data of all the clients in a job, which will be sent to server
        self.wait_send = collections.OrderedDict()
        # Backup data in files
        self.tasks_file = "./proxy/tasks.txt"  # Backup tasks
        # Backup clients that has sent basic information and will go through proxy
        self.clients_file = "./proxy/clients.txt"
        self.jobs_file = "./proxy/jobs.txt"  # Backup jobs that client has arrived at proxy
        self.data_file = "./proxy/data.txt"  # Backup sequence number of packets of each client
        self.cache_file = "./proxy/cache.txt"  # Backup data that has been arrived at
        # Load tasks from file
        self.tasks = self.load_file(self.tasks_file)
        self.tasks = collections.OrderedDict(self.tasks)
        
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
        self.time_last_lost = 0  # Recording time of losing packet last time
        self.last_retransmit = -1  # Recording sequence number of the packet last retransmitted

        self.is_in_thread = False  # Weather the thread of sending FIN to server is open

        # flow control
        self.rwnd = 1000  # Proxy rwnd
        self.server_rwnd = 1000  # Server rwnd
    
    # Load file from system
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
    
    # Send packet
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

    # Receive basic information of client from server and send ACK to server
    def client_basic_info(self, info, address):
        msg = info.msg  # Client Basic Information
        # Obtain calculation type and packet number
        client_seq = int(msg.split(delimiter)[1])
        job_id = int(msg.split(delimiter)[2])
        client_id = int(msg.split(delimiter)[3])
        cal_type = self.tasks[str(job_id)]["cal type"]
        packet_number = int(msg.split(delimiter)[4])
        weight = float(msg.split(delimiter)[5])
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

        if str(job_id) not in self.jobs.keys():
            self.jobs[str(job_id)] = []
        self.jobs[str(job_id)].append(client_id)
        if str(job_id) not in self.cache:
            self.cache[str(job_id)] = {}

    # Receive data from client and send ACK back
    def recv_data(self, pkt, address):
        client_id = str(pkt.client_id)
        job_id = str(pkt.job_id)
        # Send Ack to Client
        # Receive data
        if self.rwnd > 0 and pkt.msg.split("  ")[0] != "data":
            # Initialize
            if client_id not in self.data.keys():
                self.data[client_id] = []
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
                # Do aggregation for the data
                self.aggregate(wait_aggregation)
                self.data[client_id].append(pkt.seq)

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
            if self.clients[client_id]["correct seq"] and self.clients[client_id]["relocate seq"]:
                self.clients[client_id]["relocate seq"] = False
                for i in range(self.clients[client_id]["next seq"],
                               self.clients[client_id]["next seq"] + self.clients[client_id]["packet number"]):
                    if i not in self.data[client_id]:
                        self.clients[client_id]["next seq"] = i
                        print("Relocate: address %s now %s" %(address, i))
                        break
                msg = str(self.clients[client_id]["next seq"]) + delimiter + str(self.rwnd)
            # Send ACK to client
            self.send_packet(IS_ACK, pkt.job_id, 0, self.seq, msg, address, -1)
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
                # Record client that all the packets of it are received
                self.tasks[job_id]["arrived"].append(int(client_id))
                # If all the clients of a task send final FIN, the task will be deleted
                if set(self.tasks[str(job_id)]["arrived"]) == set(self.tasks[str(job_id)]["clients"]):
                    self.tasks.pop(job_id)
                    print("Finish task %s" % job_id)
                # Tell server all packets of a client have been received by server
                thread = threading.Thread(target=self.send_fin)
                thread.start()
    
    # self.cache{job_id:{index:[data,number,time,client_id],...},...}
    # self.data{client id:[],...}
    # self.index{"data":[[job, client, index, data],...]}
    # self.jobs{job_id:[],...}
    # Do aggregation for data recevied
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
            elif self.clients[str(a[1])]["cal type"] == "maximum":
                ans = max(self.cache[job_id][index][1], a[3])
            # Minimum
            else:
                ans = min(self.cache[job_id][index][1], a[3])
            # Handle answer
            time = self.cache[job_id][index][2]
            self.cache[job_id][index] = [ans, data_number[1] + 1, time]
        else:
            if self.clients[str(a[1])]["cal type"] == "average":
                weight = self.clients[str(a[1])]["weight"]
                ans = a[3][0] * a[3][1] * weight
            else:
                ans = a[3]
            self.cache[job_id][index] = [ans, 1, t.time()]
            self.rwnd -= 1

        clients = self.jobs[job_id]
        number = len(clients)
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
        # print("Send to Server: cwnd %s rwnd %s flight %s" %(self.cwnd,self.rwnd,self.number_in_flight))
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

    # Send data that wait to be sent to server and are in cache
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
            if min(self.cwnd, self.server_rwnd) <= self.number_in_flight:
                break

    # Clear cache when the cache is filled, i.e. rwnd = 0
    def clear_cache(self):
        self.number_in_flight = min(max(0, self.number_in_flight), len(self.packets_in_flight))
        # Send packets
        if min(self.cwnd, self.server_rwnd) > self.number_in_flight:
            while min(self.cwnd, self.server_rwnd) > self.number_in_flight:
                self.send_new_data()
                min_time = 10000000000
                job_id = 0
                # Find the earliest packet from the first packet of each job
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
                            flag = 0
                    if flag:
                        for j in self.packets_in_flight.keys():
                            if self.packets_in_flight[j]["index"] < packet_number:
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
        recv = ""
        while True:
            # If cache is filled, clear the cache
            if self.rwnd <= 0:
                self.clear_cache()
            try:
                self.sock.settimeout(3600)
                recv, address = self.sock.recvfrom(size)
            except socket.timeout:
                # Backup data
                self.backup(self.tasks_file, self.tasks)
                self.backup(self.clients_file, self.clients)
                self.backup(self.jobs_file, self.jobs)
                self.backup(self.data_file, self.data)
                self.backup(self.cache_file, self.cache)
            if recv == "":
                continue
            # Unpack the packet
            decoded_pkt = Packet(0, 0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_buf()
            # Receive basic information of clients from server
            if decoded_pkt.flag == IS_INFO:
                self.client_basic_info(decoded_pkt, address)
            # Receive data from clients
            elif decoded_pkt.flag == IS_DATA:
                self.recv_data(decoded_pkt, address)
                self.send_to_server()
            # Receive ACK from server
            elif decoded_pkt.flag == IS_ACK:
                self.receive_ack(decoded_pkt, address)
                self.send_to_server()
            # Receive FIN ACK from server
            # (The FIN packet sent to server is to tell which client's data has all been received by server)
            elif decoded_pkt.flag == IS_FIN:
                if self.clients["fin"] != {}:
                    for i in list(self.clients["fin"]):
                        if self.clients["fin"][i]["seq"] + 1 == int(decoded_pkt.msg.split(delimiter)[0]):
                            self.clients["wait fin"].pop(i)
                            self.clients["fin"].pop(i)
                            self.clients.pop(i)
                            print("No client wait FIN")
                            self.server_rwnd = int(decoded_pkt.msg.split(delimiter)[1])
                            self.data.pop(i)


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

