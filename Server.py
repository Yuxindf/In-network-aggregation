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
    def client_basic_info(self):
        # Receive basic information from client
        connection_trails_count = 0
        while 1:
            buf, address = self.sock.recvfrom(size)
            info = Packet(0, 0, 0, 0, 0, buf)
            info.decode_seq()
            # Send ACK to client
            self.offset += 1
            pkt = Packet(0, 0, self.seq, self.offset, info.seq + 1, 0)
            pkt.encode_seq()
            self.sock.sendto(pkt.buf, address)
            self.seq += 1
            job_id = info.msg.split(delimiter)[0]
            cal_type = info.msg.split(delimiter)[1]
            packet_number = info.msg.split(delimiter)[2]
            client_seq = info.seq
            # msg will include operation type, client address, client seq and size
            msg = str(address) + delimiter + str(client_seq) + delimiter + job_id + delimiter + cal_type + delimiter + packet_number

            # Send basic information to proxy
            self.offset += 1
            pkt = Packet(0, 0, self.seq, self.offset, msg, 0)
            pkt.encode_seq()
            self.sock.sendto(pkt.buf, proxy_address)
            self.seq += 1
            try:
                ack, address = self.sock.recvfrom(size)
            except:
                print("Time out reached, resending...packet number")
                continue
            ack = Packet(0, 0, 0, 0, 0, ack)
            ack.decode_seq()
            if int(ack.msg) == pkt.seq + 1:
                print("ok")
                break
            else:
                continue
        return cal_type

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
        # Listening for requests indefinitely
        while True:
            # Start - Connection initiation
            print("Starting up on %s port %s" % (serverAddress, serverPort))
            print("\nWaiting to receive message")
            if not self.handshake():
                break
            calculation_type = self.client_basic_info()
            self.receive_from_proxy()
            if len(data_list) == 1:
                self.calculate(calculation_type)
            # connectionThread = threading.Thread(target=handle_connection)
            # connectionThread.start()

    # def multithread(self):
    #     for i in range(0, 19):
    #         thread = threading.Thread(target=self.run())
    #         thread.start()


if __name__ == '__main__':
    server = Server()
    server.run()
