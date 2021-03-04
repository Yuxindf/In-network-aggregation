# udp server
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

        # Client basic information
        self.job_id = 0
        self.index = 0
        self.client_address = 0  # Connecting client address
        self.packet_number = 0  # Packet number
        self.cal_type = 0  # calculation type

        # Congestion control

    # Receive basic information of client from server and send ACK to server and client
    def client_basic_info(self):
        connection_trails_count = 0
        while 1:
            try:
                # receive basic information from server
                info, address = self.sock.recvfrom(size)
                break
            except:
                connection_trails_count += 1
                if connection_trails_count < 5:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    return False
        info = Packet(0, 0, 0, 0, 0, info)
        info.decode_seq()
        tmp = info.msg  # Client Basic Information
        # Send Ack to Server
        self.offset += 1
        pkt = Packet(0, 0, self.seq, self.offset, info.seq + 1, 0)
        pkt.encode_seq()
        self.sock.sendto(pkt.buf, address)
        self.seq += 1
        # Send Ack to Client
        msg = int(tmp.split(delimiter)[1]) + 1  # sequence number of client plus 1
        self.offset += 1
        pkt = Packet(0, 0, self.seq, self.offset, msg, 0)
        pkt.encode_seq()
        # obtain client address
        c = tmp.split(delimiter)[0]
        client_address = str(c[1:-1].split(", ")[0][1:-1])
        client_port = c[1:-1].split(", ")[1]
        client_address = (client_address, int(client_port))
        self.sock.sendto(pkt.buf, client_address)
        self.seq += 1
        # Obtain calculation type and packet number
        job_id = tmp.split(delimiter)[2]
        cal_type = tmp.split(delimiter)[3]
        packet_number = tmp.split(delimiter)[4]
        print("Receive basic information of a client %s " % c)
        print("Calculation type is %s \nNumber of Packets is %s" % (tmp.split(delimiter)[3], tmp.split(delimiter)[4]))
        client_info.append(str(client_address) + delimiter + str(job_id) + delimiter + cal_type + delimiter + str(packet_number))

    # Receive data from client and send ACK back
    def recv_data(self):
        # The file is to make a backup
        f = open("new_test1.txt", 'w')
        seq_no_flag = 1
        data_list = []
        # Receive the number of packets
        while 1:
            # Receive indefinitely
            try:
                data, client_address = self.sock.recvfrom(size)
                for i in client_info:
                    if i.split(delimiter)[0] == str(client_address):
                        self.client_address = client_address
                        self.job_id = int(i.split(delimiter)[1])
                        self.cal_type = i.split(delimiter)[2]
                        self.packet_number = int(i.split(delimiter)[3])
                        break
                connection_trails_count = 0
            except:
                connection_trails_count += 1
                if connection_trails_count < 5:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    os.remove(f)
                    break
            pkt = Packet(0, 0, 0, 0, 0, data)
            pkt.decode_seq()
            # Send Ack to Client
            if pkt.msg == "finish":
                msg = str(pkt.seq + 1) + "finish"
                data_list = np.append(data_list, pkt.msg)
            else:
                msg = pkt.seq + 1
            self.offset += 1
            ack_pkt = Packet(self.job_id, 0, self.seq, self.offset, msg, 0)
            ack_pkt.encode_seq()
            self.sock.sendto(ack_pkt.buf, self.client_address)
            self.seq += 1
            print("Receive packet %s from Client %s, sending ack..." % (pkt.seq, client_address))
            if pkt.msg == "finish":
                break
        return data_list

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
        data_list = self.recv_data()
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
            # data, address = proxy.recvfrom(size)

            self.client_basic_info()
            self.send_to_server()


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run()

