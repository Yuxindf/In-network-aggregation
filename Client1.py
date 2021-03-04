import optparse

from Packet import Packet

import socket
import numpy as np
import threading
import queue
import random
import time as t
import logging

# Set address and port
serverAddress = "127.0.0.1"
serverPort = 10000
proxyAddress = "127.0.0.1"
proxyPort = 6001
receive_window_size = 24

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"

size = 200

# State flags
CLOSED = 1
LISTEN = 2

logging.basicConfig(format='[%(asctime)s.%(msecs)03d] CLIENT - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S', filename='network.log', level=logging.INFO)


# numpy库 相当于C的数组，定义数组类型，int 8型等。。。
# tobytes，数组的一个函数。得到一个序列
# 要解序列化。 frombuffer, 转化回数组。
# numpy有相应函数：max，ave等
# 运算：加权平均；取最大；取最小。差不多了。
# 公共结构、数可以放进一个class，方便改。
# 要答辩
# Packet class definition

class Client1:
    def __init__(self, job_id, cal_type, file):
        # Client Initial State
        self.job_id = job_id
        self.seq = random.randrange(1024)  # The current sequence number
        self.index = 1
        self.offset = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.state = CLOSED

        self.file = file
        self.cal_type = cal_type  # Calculation type
        self.packet_number = 0  # The number of data that is used to calculate

        # Server and Proxy address
        self.server_address = (serverAddress, serverPort)
        self.proxy_address = (proxyAddress, proxyPort)

        # Congestion control
        self.cwnd = 3  # initial congestion window size
        self.rwnd = 1000
        self.ssthresh = 1000  #
        self.packets_in_flight = []
        self.packets_retransmit = []
        self.queue = queue.LifoQueue()  # Two threads exchange congestion window size
        self.first_seq = 0  # For each packets sending, the first sequence number of the packet
        self.finish_send = False

        self.srtt = -1
        self.devrtt = 0  # calculate the devision of srtt and real rtt
        self.rto = 0  # Retransmission timeout

        self.data_list = []

    # Three-way handshakes
    def handshake(self):
        connection_trails_count = 0
        while 1:
            print("Connect with Server " + str(serverAddress) + " " + str(serverPort))
            # first handshake
            syn = 1
            try:
                msg = "syn" + delimiter + str(syn)
                pkt = self.send_packet(msg, self.server_address)
            except:
                logging.error("Cannot send message")
            try:
                ack, address = self.sock.recvfrom(size)
            except:
                connection_trails_count += 1
                if connection_trails_count < 5:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    return False
            from_server = Packet(0, 0, 0, 0, 0, ack)
            from_server.decode_seq()
            # Third handshake
            if from_server.msg.split(delimiter)[0] == "ack number" \
                    and int(from_server.msg.split(delimiter)[1]) == pkt.seq + 1 \
                    and from_server.msg.split(delimiter)[2] == "syn" and int(from_server.msg.split(delimiter)[3]) == 1\
                    and from_server.msg.split(delimiter)[4] == "ack" and int(from_server.msg.split(delimiter)[5]) == 1:
                msg = "ack" + delimiter + str(1) + delimiter + "seq" + delimiter + str(from_server.seq + 1)
                self.send_packet(msg, address)
                self.state = LISTEN
                return True

    def open_file(self):
        try:
            file_read = open(self.file, 'r')
            print("Opening file %s" % self.file)
            data = file_read.read()
            self.data_list = data.split(" ")
            file_read.close()
        except:
            print("Requested file could not be found")

    # Store basic information to compare the efficiency between in-network aggregation and without it
    def store_basic_info(self):
        return 0

    # Send basic information to server and Receive ACK from proxy
    def send_basic_info(self):
        while 1:
            self.open_file()
            self.packet_number = len(self.data_list)
            # Send basic information to server
            # msg will include operation type, data size...
            msg = str(self.job_id) + delimiter + self.cal_type + delimiter + str(self.packet_number)  ### 类型编码成整数，占用32位或8位。header可以固定。变长也可。
            pkt = self.send_packet(msg, self.server_address)

            try:
                # Receive ACK from server
                buf, address = self.sock.recvfrom(size)
            except:
                print("Time out reached, resending...")
                continue
            ack = Packet(0, 0, 0, 0, 0, buf)
            ack.decode_seq()
            if int(ack.msg) == pkt.seq + 1:
                try:
                    # Receive ACK from proxy
                    buf, address = self.sock.recvfrom(size)
                except:
                    print("Time out reached, resending...")
                    continue
                ack = Packet(0, 0, 0, 0, 0, buf)
                ack.decode_seq()
                if int(ack.msg) == pkt.seq + 1:
                    break
                else:
                    continue
            else:
                continue

    def send_packet(self, msg, address):
        self.offset += 1
        pkt = Packet(self.job_id, self.index, self.seq, self.offset, msg, 0)
        self.seq += 1
        pkt.encode_seq()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            logging.error("Fail to send packet")
        return pkt

    def send_msg(self):
        cwnd = self.cwnd
        packet_index = 0
        self.first_seq = self.seq
        while True:
            if min(cwnd, self.rwnd) > len(self.packets_in_flight):
                # Retransmit
                while self.packets_retransmit:
                    self.send_packet(self.data_list[self.packets_retransmit[0]], self.proxy_address)
                    self.packets_in_flight.append((self.packets_retransmit[0], t.time()))
                    self.packets_retransmit.pop(0)

                self.packets_in_flight.append((packet_index, t.time()))   # 做成字典，效率高。
                print("\nSend packet with sequence number %s to proxy" % str(int(self.seq) - 1))
                msg = self.data_list[packet_index]
                self.send_packet(msg, self.proxy_address)
                packet_index += 1

            if not self.queue.empty():
                cwnd = self.queue.get()
                self.queue.queue.clear()
            print("\nCurrent cwnd %s Packets in flight %s" % (cwnd, len(self.packets_in_flight)))

            if packet_index >= len(self.data_list) and self.packets_in_flight == [] and self.packets_retransmit == []:
                self.packets_in_flight.append((-1, t.time()))
                print("\nSend finish %s to proxy")
                self.send_packet("finish", self.proxy_address)
            if self.finish_send:
                break

    # Receive ACK from proxy
    def recv_ack(self):
        while True:
            self.queue.put(self.cwnd)
            try:
                ack, address = self.sock.recvfrom(size)
            except:
                logging.error("The client does not receive ack from proxy")
            pkt = Packet(0, 0, 0, 0, 0, ack)
            pkt.decode_seq()
            seq = int(pkt.msg) - 1
            if "finish" in pkt.msg:
                self.finish_send = True
            flag_digit = seq - self.first_seq
            for i in range(0, len(self.packets_in_flight)):
                if self.packets_in_flight[i][0] == flag_digit:
                    send_time = self.packets_in_flight[i][1]
                    self.packets_in_flight.pop(i)
                    break

            print("\nReceive ack of packet with sequence number %s" % str(int(pkt.msg)-1))
            # Slow start
            if self.cwnd < self.ssthresh:
                self.cwnd += 1
            # Congestion control
            else:
                self.cwnd += 1.0 / self.cwnd

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
                self.srtt = (1 - alpha) * self.srtt + alpha * rtt
                self.devrtt = (1 - beta) * self.devrtt + beta * abs(rtt - self.srtt)
                self.rto = self.srtt + max(0.010, 4 * self.devrtt)
                print("rto: %s" % self.rto)
            # Situation of losing packets
            for i in range(0, len(self.packets_in_flight)):
                if t.time() - self.packets_in_flight[i][1] >= self.rto:
                    self.packets_retransmit.append(self.packets_in_flight[0][0])
                    self.cwnd = 3
                    self.ssthresh = 1/2 * self.ssthresh
                    print("Retransmit")
                else:
                    break

    # Receive result from server
    def result_from_server(self):
        result = ""
        try:
            # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
            self.sock.settimeout(4)
            result, address = self.sock.recvfrom(size)
        except:
            print("Internal Server Error")
        print(result.decode())

    def run(self):
            # flag = False
        # # Connection initiation
        # while 1:
        #     if self.state == CLOSED:
        #         logging.info("Handshaking...")
        #         flag = True
        #         self.handshake()
        #     elif self.state == LISTEN:
        #         pass
        #     if flag == True:
                self.handshake()
                userInput = input("\nInput file and Calculation type: ")
                # self.job_id = int(userInput.split(" ")[0])
                # self.cal_type = userInput.split(" ")[1]
                # self.file = userInput.split(" ")[2]
                # print("Requesting the %s in file %s" % (userInput.split(" ")[1], userInput.split(" ")[2]))
                print("Requesting the %s in file %s" % (self.cal_type, self.file))
                self.send_basic_info()
                # self.sock.sendto("Start sending".encode(), proxy_address)
                send_msg = threading.Thread(target=self.send_msg)
                recv_msg = threading.Thread(target=self.recv_ack)
                send_msg.start()
                recv_msg.start()
                flag = False

                # self.send_data(userInput.split(",")[0])
            # self.result_from_server()


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-j', dest='job', type='int', default=1)
    parser.add_option('-t', dest='type', default="sum")
    parser.add_option('-f', dest='file', default="test1.txt")
    (options, args) = parser.parse_args()
    client = Client1(options.job, options.type, options.file)
    client.run()

    # finally:
    #     print("Closing socket")
    #     sock.close()
    #     break
