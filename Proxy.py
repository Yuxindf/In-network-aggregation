# udp server
import collections
import numpy as np
import socket
import hashlib
import os
import random
import json
import threading
from Packet import Packet

host = '127.0.0.1'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port
server_address = (host, server_port)

wait_ack_list = []
result_list = []
client_info = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200


class Proxy:
    def __init__(self):
        # Proxy initial state
        self.seq = random.randrange(1024)  # The current sequence number
        self.offset = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, proxy_port))

        self.clients = collections.OrderedDict()
        self.data = collections.OrderedDict()
        self.calculate = collections.OrderedDict()

        # Backup data in files
        self.clients_file = "./proxy/clients.txt"
        self.data_file = "./proxy/data.txt"
        self.calculate_file = "./proxy/wait_calculate.txt"

        # Client basic information

        # flow control
        self.rwnd = 1000

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

    # Receive basic information of client from server and send ACK to server and client
    def client_basic_info(self, info, address):
        msg = info.msg  # Client Basic Information
        # Obtain calculation type and packet number
        job_id = int(msg.split(delimiter)[3])
        client_id = int(msg.split(delimiter)[4])
        cal_type = msg.split(delimiter)[5]
        packet_number = int(msg.split(delimiter)[6])
        # Send Ack to Server
        ack = "proxy ack" + delimiter + str(client_id) + delimiter + str(info.seq + 1)
        self.send_packet(ack, address)
        # obtain client address
        c = msg.split(delimiter)[1]
        client_address = str(c[1:-1].split(", ")[0][1:-1])
        client_port = c[1:-1].split(", ")[1]
        client_address = (client_address, int(client_port))
        print("Receive basic information of a client %s " % c)
        print("Calculation type is %s \nNumber of Packets is %s" % (cal_type, packet_number))
        self.clients[str(client_id)] = {"address": client_address, "job id": job_id, "cal type": cal_type, "packet number": packet_number}
        # Backup clients' basic information in a file
        self.backup(self.clients_file, self.clients)

    # Receive data from client and send ACK back
    def recv_data(self, pkt, address):
        # Send Ack to Client
        if "finish" not in pkt.msg:
            # Initialize
            if str(pkt.client_id) not in self.data.keys():
                self.data[str(pkt.client_id)] = {"data": []}
                # Backup data from clients in a file
                self.backup(self.data_file, self.data)
            packet_index = int(pkt.msg.split(delimiter)[2])
            data = pkt.msg.split(delimiter)[1]
            # Weighted average
            if self.clients[str(pkt.client_id)]["cal type"] == "average":
                number = float(data[1:-1].split(",")[0])
                weight = float(data[1:-1].split(",")[1])
                data = (number, weight)
                self.data[str(pkt.client_id)]["data"].append(data)
            # Maximum or Minimum
            else:
                data = float(data)
                self.data[str(pkt.client_id)]["data"].append(data)
            # Update data from clients in the file
            self.backup(self.data_file, self.data)
            msg = str(pkt.seq + 1) + delimiter + str(self.rwnd) + delimiter + str(packet_index)
            self.send_packet(msg, address)
            # print("Receive packet %s from Client %s, sending ack..." % (packet_index, address))

        else:
            msg = str(pkt.seq + 1) + "finish"
            print("Receive all packets from client (%s, %s)" % address)
            self.send_packet(msg, address)
            # Update data from clients in the file
            self.backup(self.data_file, self.data)
            self.calculate[str(pkt.client_id)] = {}
            # Backup data that wait to be calculated in a file
            self.backup(self.calculate_file, self.calculate)
            print(self.data)

    # 带权平均，packet header可以带一个权重，默认为1。server那里可知，做到全局平均。
    # do some calculations
    def aggregate(self):
        if self.calculate != {}:
            for key in self.calculate.keys():
                # Weighted average
                if self.clients[key]["cal type"] == "average":
                    total = 0
                    for i in self.data[key]["data"]:
                        total += i[0] * i[1]
                    ans = total / len(self.data[key]["data"])
                # Weighted average
                else:
                    data = []
                    for i in self.data[key]["data"]:
                        data = np.append(data, i)
                    if self.clients[key]["cal type"] == "maximum":
                        ans = data.max()
                    elif self.clients[key]["cal type"] == "minimum":
                        ans = data.min()
                print(ans)
                msg = "aggregation result" + delimiter + str(key) + delimiter + str(ans)
                self.send_packet(msg, server_address)
                self.data.pop(key)
                # Update data
                self.backup(self.data_file, self.data)
                self.calculate.pop(key)
                # Update data that wait to be calculated in the file
                self.backup(self.calculate_file, self.calculate)
                # Update clients
                self.clients.pop(key)
                self.backup(self.clients_file, self.clients)

    def run(self):
        print("Proxy start up on %s port %s\n" % (host, proxy_port))
        # Load data
        self.clients = self.load_file(self.clients_file)
        self.data = self.load_file(self.data_file)
        self.calculate = self.load_file(self.calculate_file)
        self.clients = collections.OrderedDict(self.clients)
        self.data = collections.OrderedDict(self.data)
        self.calculate = collections.OrderedDict(self.calculate)
        while 1:
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            if "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)
            elif "data" in decoded_pkt.msg:
                self.recv_data(decoded_pkt, address)
                self.aggregate()


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

