import collections
import socket
import threading
import hashlib
import time
import os
import random
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


class Server:
    def __init__(self):
        self.seq = random.randrange(1024)  # The current sequence number
        self.offset = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((serverAddress, serverPort))  # Bind the socket to the port

        self.clients = collections.OrderedDict()

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

    # Three-way handshakes
    def handshake(self):
        connection_trails_count = 0
        while 1:
            # Second handshake
            try:
                recv, address = self.sock.recvfrom(size)
                print("Connecting with client " + str(address))
            except:
                connection_trails_count += 1
                if connection_trails_count < 5:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    return False
            ack_packet = Packet(0, 0, 0, 0, 0, recv)
            ack_packet.decode_seq()
            if ack_packet.msg.split(delimiter)[0] == "syn" and int(ack_packet.msg.split(delimiter)[1]) == 1:
                # Send to client
                msg = "ack number" + delimiter + str(ack_packet.seq + 1) + delimiter + "syn" + delimiter + str(1) + \
                      delimiter + "ack" + delimiter + str(1)
                self.offset += 1
                send_packet = Packet(0, 0, self.seq, self.offset, msg, 0)
                send_packet.encode_seq()
                self.sock.sendto(send_packet.buf, address)
                self.seq += 1
            try:
                recv, address = self.sock.recvfrom(size)
            except:
                print("Internal Server Error")
            from_client = Packet(0, 0, 0, 0, 0, recv)
            from_client.decode_seq()

            # Third handshake
            if from_client.msg.split(delimiter)[0] == "ack" and int(from_client.msg.split(delimiter)[1]) == 1 \
                    and from_client.msg.split(delimiter)[2] == "seq"\
                    and int(from_client.msg.split(delimiter)[3]) == send_packet.seq + 1:
                print("Successfully connect with " + str(address))
                return True

    # Receive basic information from client and send to proxy
    def client_basic_info(self, info, address):
        # Receive basic information from client
        connection_trails_count = 0
        client_seq = info.seq
        cal_type = info.msg.split(delimiter)[1]
        packet_number = info.msg.split(delimiter)[2]
        self.clients["job id"] = info.job_id
        self.clients["cal type"] = info.msg.split(delimiter)[1]
        self.clients["packet number"] = info.msg.split(delimiter)[2]

        # Send basic information to proxy
        # msg will include operation type, client address, client seq and size
        msg = "client info" + delimiter + str(address) + delimiter + str(client_seq) + delimiter + str(info.job_id) +\
              delimiter + str(info.client_id) + delimiter + cal_type + delimiter + packet_number
        self.clients[info.client_id]["server seq"] = self.seq
        self.clients[info.client_id]["client seq"] = info.seq
        self.send_packet(msg, proxy_address)

    # Receive result from proxy
    def receive_from_proxy(self):
        result = ""
        try:
            # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
            result, address = self.sock.recvfrom(size)
        except:
            print("Internal Server Error")
        print(result.decode())
        client_result = result.decode()
        client = client_result.split(delimiter)[0]
        # obtain client address
        client_address = str(client[1:-1].split(", ")[0][1:-1])
        client_port = client[1:-1].split(", ")[1]
        client = (client_address, int(client_port))
        # obtain data
        result = client_result.split(delimiter)[1]
        # Store data
        data_list.append(result)
        # Store client address
        client_list.append(client)

    # do some calculations
    def calculate(self, cal_type):
        total = 0
        count = 0
        for a in data_list[::-1]:
            data_list.remove(a)
            count += 1
            total = total + float(a)

        ave = total / count
        print("the average is", ave)

        try:
            for addr in client_list[::-1]:
                print("Send to client " + str(addr))
                if cal_type == "average":
                    self.sock.sendto(("ave is " + str(ave)).encode(), addr)
                elif cal_type == "sum":
                    self.sock.sendto(("sum is " + str(total)).encode(), addr)
        except:
            print("Internal Server Error")

    def run(self):
        print("Starting up on %s port %s" % (serverAddress, serverPort))
        print("\nWaiting to receive message")
        # Listening for requests indefinitely
        while True:
            # Start - Connection initiation
            recv, address = self.sock.recvfrom(size)
            decoded_pkt = Packet(0, 0, 0, 0, 0, recv)
            decoded_pkt.decode_seq()
            # First and Second Handshake
            if "syn" in decoded_pkt.msg and str(1) in decoded_pkt.msg:
                msg = "ack number" + delimiter + str(decoded_pkt.seq + 1) + delimiter + "syn" + delimiter + str(1) + \
                      delimiter + "ack" + delimiter + str(1)
                self.send_packet(msg, address)
                server_seq = self.seq - 1
                self.clients[decoded_pkt.client_id] = {"address": address, "server seq": server_seq - 1}
            # Third handshake
            elif "client ack" in decoded_pkt.msg and decoded_pkt.msg.split(delimiter)[1] ==1:
                last_seq = int(decoded_pkt.msg.split(delimiter)[3])
                if self.clients[decoded_pkt.client_id]["server seq"] + 1 == last_seq:
                    self.clients[decoded_pkt.client_id]["state"] = "connected"

            # Receive client basic information
            elif "client info" in decoded_pkt.msg:
                self.client_basic_info(decoded_pkt, address)
            elif "proxy ack" in decoded_pkt.msg:
                client_id = int(decoded_pkt.msg.split(delimiter)[1])
                if int(decoded_pkt.msg.split(delimiter)[2]) == self.clients[client_id]["server seq"] + 1:
                    # Send Ack to client
                    self.send_packet(self.clients[client_id]["client seq"] + 1, self.clients[client_id]["address"])
                    print("okkk")
            # calculation_type = self.client_basic_info()
            # self.receive_from_proxy()
            # if len(data_list) == 1:
            #     self.calculate(calculation_type)
            # connectionThread = threading.Thread(target=handle_connection)
            # connectionThread.start()


    # def multithread(self):
    #     for i in range(0, 19):
    #         thread = threading.Thread(target=self.run())
    #         thread.start()


if __name__ == '__main__':
    server = Server()
    server.run()
