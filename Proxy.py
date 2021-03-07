# udp server
import collections
import numpy as np
import socket
import hashlib
import os
import random
from Packet import Packet

host = '127.0.0.1'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port

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
    def recv_data(self, data, address):
        # The file is to make a backup
        f = open("new_test1.txt", 'w')
        seq_no_flag = 1
        data_list = []

        # Send Ack to Client
        if data.msg == "finish":
            msg = str(data.seq + 1) + "finish"
        else:
            packet_index = data.msg.split(delimiter)[2]
            print(float(data.msg.split(delimiter)[1]))
            data_list = np.append(data_list, float(data.msg.split(delimiter)[1]))
        self.offset += 1
        msg = str(data.seq + 1) + delimiter + str(self.rwnd) + delimiter + str(packet_index)
        ack_pkt = Packet(0, 0, self.seq, self.offset, msg, 0)
        ack_pkt.encode_seq()
        self.sock.sendto(ack_pkt.buf, address)
        self.seq += 1
        print("Receive packet %s from Client %s, sending ack..." % (data.seq, address))
        if data.msg == "finish":
            print("Receive all packets from client")

    # 带权平均，packet header可以带一个权重，默认为1。server那里可知，做到全局平均。
    # do some calculations
    def calculate(self, index, data_list):
        # with open("new_test1.txt", "r") as f:
        #     for line in f:
        #         word_list = line.split(space)
        #         for a in word_list[:-1]:
        #             if int(index) == int(a.split(delimiter)[1]):
        #
        # f.close()
        ans = 0
        if self.cal_type == "maximum":
            ans = data_list.max()
        elif self.cal_type == "minimum":
            ans = data_list.min()
        elif self.cal_type == "weighted average":
            sum = 0
            for i in data_list[::-1]:
                sum += data_list[i]
            ans = sum/len(data_list)
        return ans

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
        print(host + " Client connect to Server %s\n" % server_port)
        while 1:
            print("\nWaiting to receive message")
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            if "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)
            elif "data" in decoded_pkt.msg:
                self.recv_data(decoded_pkt, address)


            # self.send_to_server()


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

