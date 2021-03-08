# udp server
import collections
import numpy as np
import socket
import hashlib
import os
import random
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
        self.send_aggregation = collections.OrderedDict()

        # Client basic information

        # flow control
        self.rwnd = 1000

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
        self.clients[client_id] = {"address": client_address, "job id": job_id, "cal type": cal_type, "packet number": packet_number}

    # Receive data from client and send ACK back
    def recv_data(self, pkt, address):
        # The file is to make a backup
        f = open("new_test1.txt", 'w')
        seq_no_flag = 1
        data_list = []
        # Send Ack to Client
        if "finish" not in pkt.msg:
            # Initialize
            if pkt.client_id not in self.data.keys():
                self.data[pkt.client_id] = {"data": []}
            packet_index = int(pkt.msg.split(delimiter)[2])
            data = pkt.msg.split(delimiter)[1]
            # Weighted average
            if self.clients[pkt.client_id]["cal type"] == "average":
                number = float(data[1:-1].split(",")[0])
                weight = float(data[1:-1].split(",")[1])
                data = (number, weight)
                self.data[pkt.client_id]["data"].append(data)
            # Maximum or Minimum
            else:
                data = float(data)
                self.data[pkt.client_id]["data"] = np.append(self.data[pkt.client_id]["data"], data)
            msg = str(pkt.seq + 1) + delimiter + str(self.rwnd) + delimiter + str(packet_index)
            self.send_packet(msg, address)
            print("Receive packet %s from Client %s, sending ack..." % (packet_index, address))

        else:
            msg = str(pkt.seq + 1) + "finish"
            print("Receive all packets from client")
            self.send_packet(msg, address)
            self.calculate[pkt.client_id] = {}
            print(self.data)

    # 带权平均，packet header可以带一个权重，默认为1。server那里可知，做到全局平均。
    # do some calculations
    def aggregate(self):
        # with open("new_test1.txt", "r") as f:
        #     for line in f:
        #         word_list = line.split(space)
        #         for a in word_list[:-1]:
        #             if int(index) == int(a.split(delimiter)[1]):
        #
        # f.close()
        if self.calculate != {}:
            for key in self.calculate.keys():
                if self.clients[key]["cal type"] == "maximum":
                    ans = self.data[key]["data"].max()
                elif self.clients[key]["cal type"] == "minimum":
                    ans = self.data[key]["data"].min()
                elif self.clients[key]["cal type"] == "average":
                    sum = 0
                    for i in self.data[key]["data"]:
                       sum += i[0] * i[1]
                    ans = sum / len(self.data[key]["data"])
                print(ans)
                self.send_aggregation[key] = {"result": ans}
                msg = "aggregation result" + delimiter + str(key) + delimiter + str(ans)
                self.send_packet(msg, server_address)
                self.calculate.pop(key)


    def send_to_server(self):

        ave = self.calculate(self.index)
        packet = str(self.client_address) + delimiter + str(ave)
        server_address = (host, server_port)
        try:
            print("Send to server")
            self.sock.sendto(packet.encode(), server_address)
        except:
            print("Internal Server Error")

    def run(self):
        print("Proxy start up on %s port %s\n" % (host, proxy_port))
        while 1:
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            if "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)
            elif "data" in decoded_pkt.msg:
                self.recv_data(decoded_pkt, address)
                self.aggregate()




            # self.send_to_server()


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

