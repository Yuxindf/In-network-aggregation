
from Packet import Packet

import collections
import socket
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
CONNECTED = 3
FIN = 4

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
    def __init__(self):
        self.client_id = 1
        # Client Initial State
        self.job_id = 0
        self.seq = random.randrange(1024)  # The current sequence number
        self.index = 1
        self.offset = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.state_history = CLOSED
        self.state = CLOSED

        self.file = 0
        self.cal_type = 0  # Calculation type
        self.weight = -1  # Used for weighted average
        self.packet_number = 0  # The number of data that is used to calculate

        # Server and Proxy address
        self.server_address = (serverAddress, serverPort)
        self.proxy_address = (proxyAddress, proxyPort)

        # Congestion control
        self.cwnd = 3  # initial congestion window size
        self.rwnd = 1000
        self.ssthresh = 1000  #
        self.packet_index = 0
        self.number_in_flight = 0
        self.last_retransmit = 0
        self.packets_in_flight = collections.OrderedDict()
        self.packets_retransmit = collections.OrderedDict()

        self.srtt = -1  # Smooth round-trip timeout
        self.devrtt = 0  # calculate the devision of srtt and real rtt
        self.rto = 10  # Retransmission timeout
        self.time_last_lost = 0

        self.data_list = collections.OrderedDict()

    # Three-way handshakes
    def handshake(self):
        connection_trails_count = 0
        while True:
            print("Connect with Server " + str(serverAddress) + " " + str(serverPort))
            # first handshake
            syn = 1
            # try:
            msg = "SYN" + delimiter + str(syn)
            try:
                pkt = self.send_packet(msg, self.server_address, -1)
            except:
                logging.error("Cannot send message")
            try:
                ack, address = self.sock.recvfrom(size)
            except socket.timeout:
                connection_trails_count += 1
                if connection_trails_count < 10:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    return
            from_server = Packet(0, 0, 0, 0, 0, ack)
            from_server.decode_seq()
            # Third handshake
            if from_server.msg.split(delimiter)[0] == "ack number" \
                    and int(from_server.msg.split(delimiter)[1]) == pkt.seq + 1 \
                    and from_server.msg.split(delimiter)[2] == "SYN" and int(from_server.msg.split(delimiter)[3]) == 1\
                    and from_server.msg.split(delimiter)[4] == "ACK" and int(from_server.msg.split(delimiter)[5]) == 1:
                msg = "SYN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(from_server.seq + 1)
                self.send_packet(msg, address, -1)
                if self.state_history == CLOSED:
                    self.state_history = CONNECTED
                self.state = self.state_history
                print("Successfully connected")
                break

    def open_file(self):
        try:
            file_read = open(self.file, 'r')
            print("Opening file %s" % self.file)
            data = file_read.read()
            index = 0
            for i in data.split(" "):
                self.data_list[index] = {"data": i, "duplicate acks": 0, "seq": -1}
                index += 1
            file_read.close()
        except FileNotFoundError:
            print("Requested file could not be found")

    # Store basic information to compare the efficiency between in-network aggregation and without it
    def store_basic_info(self):
        return 0

    # Send basic information to server and Receive ACK from server
    def send_basic_info(self):
        connection_trails_count = 0
        while True:
            self.open_file()
            self.packet_number = len(self.data_list)
            # Send basic information to server
            # msg will include operation type, data size...
            if self.weight == -1:
                msg = "client info" + delimiter + self.cal_type + delimiter + str(self.packet_number)  ### 类型编码成整数，占用32位或8位。header可以固定。变长也可。
            else:
                msg = "client info" + delimiter + self.cal_type + delimiter + str(self.packet_number) + delimiter + str(self.weight)
            pkt = self.send_packet(msg, self.server_address, -1)
            try:
                # self.sock.settimeout(5)
                # Receive ACK from server
                buf, address = self.sock.recvfrom(size)
            except socket.timeout:
                print("The connection is closed. Start connecting again...\n")
                self.state = CLOSED
                break
            ack = Packet(0, 0, 0, 0, 0, buf)
            ack.decode_seq()
            if int(ack.msg) == pkt.seq + 1:
                break
            else:
                continue

    def send_packet(self, msg, address, seq):
        self.offset += 1
        if seq == -1:
            pkt = Packet(self.job_id, self.client_id, self.seq, self.offset, msg, 0)
            self.seq += 1
        else:
            pkt = Packet(self.job_id, self.client_id, seq, self.offset, msg, 0)
        pkt.encode_seq()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            logging.error("Fail to send packet")
        return pkt

    def send_data(self):
        # Send message
        if self.data_list == {}:
            self.packets_in_flight[self.packet_index] = {"seq": self.seq, "time": t.time()}
            self.number_in_flight += 1
            print("\nSend finish to server")
            self.send_packet("data: finish", self.server_address, -1)

        # Send packets
        print("%s %s %s %s" % (self.cwnd, self.number_in_flight, len(self.packets_in_flight), len(self.data_list)))
        self.number_in_flight = min(max(0, self.number_in_flight), len(self.packets_in_flight))
        if min(self.cwnd, self.rwnd) > self.number_in_flight and self.data_list != {}:
            while min(self.cwnd, self.rwnd) > self.number_in_flight:
                # Retransmit: Three duplicate acks.
                for key in self.data_list.keys():
                    if self.data_list[key]["duplicate acks"] >= 3:
                        msg = "data" + delimiter + str(self.data_list[key]["data"])
                        self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                        self.number_in_flight += 1
                        self.send_packet(msg, self.server_address, self.data_list[key]["seq"])
                        self.data_list[key]["duplicate acks"] = 0
                        if key != self.last_retransmit and t.time() - self.time_last_lost > self.srtt:
                            self.cwnd /= 2
                            self.ssthresh /= 2
                            self.last_retransmit = key
                            self.time_last_lost = t.time()
                    break

                # Retransmit: timeout
                if self.packets_retransmit != {}:
                    for key in list(self.packets_retransmit.keys()):
                        msg = "data" + delimiter + str(self.data_list[key]["data"])
                        self.number_in_flight += 1
                        self.send_packet(msg, self.server_address, self.data_list[key]["seq"])
                        self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                        self.packets_retransmit.pop(key)
                        print("retransmit %s" % key)

                # Send packet
                if self.packet_index < self.packet_number:
                    if self.data_list[self.packet_index]["seq"] == -1:
                        self.data_list[self.packet_index]["seq"] = self.seq
                    self.packets_in_flight[self.packet_index] = {"seq": self.data_list[self.packet_index]["seq"], "time": t.time()}  # 做成字典，效率高。
                    self.number_in_flight += 1
                    # print("Send to server: Packet index %s Seq %s" % (self.packet_index, str(int(self.seq))))
                    # print(self.packets_in_flight.keys())
                    msg = "data" + delimiter + str(self.data_list[self.packet_index]["data"])
                    self.send_packet(msg, self.server_address, -1)
                    self.send_packet(msg, self.server_address, -1)
                    self.packet_index += 1
                    # print(self.packets_in_flight)
                else:
                    break

        # print("\nCurrent cwnd %s Packets in flight %s" % (self.cwnd, len(self.packets_in_flight)))

    def receive_ack(self):
        ack = ""
        # Receive ack from server
        try:
            self.sock.settimeout(self.rto)
            ack, address = self.sock.recvfrom(size)
        except socket.timeout:
            logging.info("The client does not receive ack from server")
        if ack != "":
            pkt = Packet(0, 0, 0, 0, 0, ack)
            pkt.decode_seq()
            # print(pkt.msg)
            self.number_in_flight -= 1
            # Slow start
            if self.cwnd < self.ssthresh:
                self.cwnd += 1
            # Congestion control
            else:
                self.cwnd += 1.0 / self.cwnd
            # print(pkt.msg.split(delimiter)[2])
            if "finish" not in pkt.msg:
                next_seq = int(pkt.msg.split(delimiter)[0])
                self.rwnd = int(pkt.msg.split(delimiter)[1])
                send_time = 0
                if self.number_in_flight != 0:
                    # print(len(self.packets_in_flight))
                    # Remove data that are ensured to be received
                    tmp_index = -1

                    for i in list(self.data_list.keys()):
                        if self.data_list[i]["seq"] != -1 and self.data_list[i]["seq"] <= next_seq - 1:
                            self.data_list.pop(i)
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

                            # print(self.data_list)
                        elif self.data_list[i]["seq"] == next_seq:
                            tmp_index = i
                            break
                        elif self.data_list[i]["seq"] == -1:
                            self.data_list[i]["seq"] = next_seq
                            tmp_index = i
                            break

                    if tmp_index != -1:
                        # Record duplicate ack
                        self.data_list[tmp_index]["duplicate acks"] += 1

                        # print("rto: %s \n srtt : %s" % (self.rto, self.srtt))
            # Finish sending data
            else:
                self.state_history = FIN
                self.state = FIN

        # Situation of losing packets
        for key in list(self.packets_in_flight.keys()):
            if t.time() - self.packets_in_flight[key]["time"] >= self.rto:  # For test
                self.packets_retransmit[key] = ""
                self.packets_in_flight.pop(key)
                if t.time() - self.time_last_lost > self.srtt:
                    self.ssthresh = 1 / 2 * self.cwnd
                    self.cwnd = 3
                    self.time_last_lost = t.time()
                print("Retransmit: packet index %s" % key)
            else:
                break

    # Receive result from server and four waves
    def disconnect(self):
        connection_trails_count = 0
        while True:
            # First wave
            msg = "FIN" + delimiter + str(1)
            self.send_packet(msg, self.server_address, -1)
            try:
                ack, address = self.sock.recvfrom(size)
            except socket.timeout:
                self.state = CLOSED
            pkt = Packet(0, 0, 0, 0, 0, ack)
            pkt.decode_seq()
            # Second wave
            if pkt.msg.split(delimiter)[0] == "FIN ACK" and int(pkt.msg.split(delimiter)[1]) == 1 \
                    and pkt.msg.split(delimiter)[2] == "ack number" and int(pkt.msg.split(delimiter)[3]) == self.seq:
                break
            else:
                continue
        # Receive final result from server
        # self.sock.settimeout(60)
        try:
            self.sock.settimeout(120)
            result, address = self.sock.recvfrom(size)
        except socket.timeout:
            self.state = CLOSED
            print("Do not receive final result. Reconnecting...")
        pkt = Packet(0, 0, 0, 0, 0, result)
        pkt.decode_seq()
        result = pkt.msg
        print("Final result %s" % result)

        try:
            fin, address = self.sock.recvfrom(size)
        except socket.timeout:
            print("The connection is closed.")
        pkt = Packet(0, 0, 0, 0, 0, fin)
        pkt.decode_seq()
        # Third wave
        if pkt.msg.split(delimiter)[0] == "FIN" and int(pkt.msg.split(delimiter)[1]) == 1 \
                and pkt.msg.split(delimiter)[2] == "ACK" and int(pkt.msg.split(delimiter)[3]) == 1 \
                and pkt.msg.split(delimiter)[4] == "ack number" and int(pkt.msg.split(delimiter)[5]) == self.seq:
            msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(pkt.seq + 1)
            # Fourth wave
            self.send_packet(msg, address, -1)
            t.sleep(0.002)
            self.sock.close()
            print("close socket")

    def run(self):
        # Connection initiation
        count = 0
        while True:
            if self.state == CLOSED:
                logging.info("Handshaking...")
                self.handshake()
                userInput = "1 minimum test1.txt"
                # userInput = "1 average test2.txt 0.1"
                # userInput = input("\nInput file and Calculation type: ")
                self.job_id = int(userInput.split(" ")[0])
                self.cal_type = userInput.split(" ")[1]
                self.file = userInput.split(" ")[2]
                if self.cal_type == "average":
                    self.weight = userInput.split(" ")[3]

                # print("Requesting the %s in file %s" % (userInput.split(" ")[1], userInput.split(" ")[2]))
                print("Requesting the %s in file %s" % (self.cal_type, self.file))
                self.send_basic_info()
            elif self.state == LISTEN:
                pass
            elif self.state == CONNECTED:
                self.send_data()
                self.receive_ack()
            elif self.state == FIN:
                self.disconnect()
                break


if __name__ == '__main__':
    client = Client1()
    client.run()
