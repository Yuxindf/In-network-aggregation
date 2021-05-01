import json

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

size = 200

# State flags
CLOSED = 1
LISTEN = 2
CONNECTED = 3
FIN = 4

# Data type flags
IS_SYN = 1
IS_INFO = 2
IS_DATA = 3
IS_ACK = 4
IS_FIN = 5

logging.basicConfig(format='[%(asctime)s.%(msecs)03d] CLIENT - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S', filename='network.log', level=logging.INFO)


class Client2:
    def __init__(self):
        self.last= 0
        self.client_id = 2
        # Client Initial State
        self.job_id = 0
        self.seq = random.randrange(1024)  # Initial sequence number(ISN) should be random
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

        # Flow control
        self.rwnd = 1000

        self.data_list = collections.OrderedDict()
        # To draw diagrams
        self.info = collections.OrderedDict()
        self.info_file = "./client/info.txt"

    # Backup
    def backup(self, file, data):
        with open(file, "w") as f:
            store = json.dumps(data)
            f.write(store)
        f.close()

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
                pkt = self.send_packet(IS_SYN, msg, self.server_address, -1, -1)
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
            from_server = Packet(0, 0, 0, 0, 0, 0, ack)
            from_server.decode_seq()
            # Third handshake
            if from_server.flag == IS_SYN and from_server.msg.split(delimiter)[0] == "ack number" \
                    and int(from_server.msg.split(delimiter)[1]) == pkt.seq + 1 \
                    and from_server.msg.split(delimiter)[2] == "SYN" and int(from_server.msg.split(delimiter)[3]) == 1\
                    and from_server.msg.split(delimiter)[4] == "ACK" and int(from_server.msg.split(delimiter)[5]) == 1:
                msg = "SYN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(from_server.seq + 1)
                self.send_packet(IS_SYN, msg, address, -1, -1)
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
                self.data_list[index] = {"data": i, "duplicate acks": 0}
                index += 1
            file_read.close()
        except FileNotFoundError:
            print("Requested file could not be found")

    # Store basic information to compare the efficiency between in-network aggregation and without it
    def store_basic_info(self):
        return 0

    # Send basic information to server and Receive ACK from proxy
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
            pkt = self.send_packet(IS_INFO, msg, self.server_address, -1, -1)
            try:
                # self.sock.settimeout(5)
                # Receive ACK from server
                buf, address = self.sock.recvfrom(size)
            except socket.timeout:
                print("The connection is closed. Start connecting again...\n")
                self.state = CLOSED
                break
            ack = Packet(0, 0, 0, 0, 0, 0, buf)
            ack.decode_seq()
            if int(ack.msg) == pkt.seq + 1:
                break
            else:
                continue

    def send_packet(self, flag, msg, address, seq, index):
        if seq == -1:
            pkt = Packet(flag, self.job_id, self.client_id, self.seq, index, msg, 0)
            self.seq += 1
        else:
            pkt = Packet(flag, self.job_id, self.client_id, seq, index, msg, 0)
        pkt.encode_seq()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            logging.error("Fail to send packet")
        return pkt

    def send_data(self):
        # Number of packets in flight
        self.number_in_flight = min(max(0, self.number_in_flight), len(self.packets_in_flight))

        # Send packets
        # print("rwnd %s cwnd %s in flight %s remaining %s" % (self.rwnd, self.cwnd, len(self.packets_in_flight), len(self.data_list)))
        if min(self.cwnd, self.rwnd) > self.number_in_flight and self.data_list != {}:
            while min(self.cwnd, self.rwnd) > self.number_in_flight:
                # Retransmit: Three duplicate acks.
                for key in self.data_list.keys():
                    if self.data_list[key]["duplicate acks"] >= 3:
                        msg = "data" + delimiter + str(self.data_list[key]["data"])
                        self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                        self.number_in_flight += 1
                        self.send_packet(IS_DATA, msg, self.proxy_address, self.data_list[key]["seq"], key)
                        self.data_list[key]["duplicate acks"] = 0
                        if key != self.last_retransmit and t.time() - self.time_last_lost > self.srtt:
                            self.cwnd /= 2
                            self.ssthresh /= 2
                            self.last_retransmit = key
                            self.time_last_lost = t.time()
                    break
                if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                    break

                # Retransmit: timeout
                if self.packets_retransmit != {}:
                    for key in list(self.packets_retransmit.keys()):
                        if key in self.data_list:
                            msg = "data" + delimiter + str(self.data_list[key]["data"])
                            self.number_in_flight += 1
                            self.send_packet(IS_DATA, msg, self.proxy_address, self.data_list[key]["seq"], key)
                            self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                            self.packets_retransmit.pop(key)
                            print("Retransmit: packet index %s" % key)
                            if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                                break

                if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                    break

                # Send data
                if self.packet_index < self.packet_number:
                    self.packets_in_flight[self.packet_index] = {"seq": self.data_list[self.packet_index]["seq"], "time": t.time()}  # 做成字典，效率高。
                    self.number_in_flight += 1
                    # print("Send to proxy: Packet index %s Seq %s" % (self.packet_index, str(int(self.seq))))
                    # print(self.packets_in_flight.keys())
                    msg = "data" + delimiter + str(self.data_list[self.packet_index]["data"]) + delimiter
                    self.send_packet(IS_DATA, msg, self.proxy_address, self.data_list[self.packet_index]["seq"], self.packet_index)
                    self.packet_index += 1
                    # print(self.packets_in_flight)
                else:
                    break

        elif self.rwnd == 0:
            t.sleep(0.1)
            self.send_packet(IS_DATA, "data", self.proxy_address, self.seq, -1)

        # print("\nCurrent cwnd %s Packets in flight %s" % (self.cwnd, len(self.packets_in_flight)))
        # print("\nCurrent cwnd %s Packets in flight %s" % (self.cwnd, len(self.packets_in_flight)))

    def receive_ack(self):
        ack = ""
        # Receive ack from proxy
        try:
            ack, address = self.sock.recvfrom(size)
        except:
            logging.info("The client does not receive ack from proxy")
        if ack != "":
            pkt = Packet(0, 0, 0, 0, 0, 0, ack)
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
            # Receive ack of data
            next_seq = int(pkt.msg.split(delimiter)[0])
            self.rwnd = int(pkt.msg.split(delimiter)[1])
            if self.number_in_flight > -1:
                # Remove data that are ensured to be received
                for i in list(self.data_list.keys()):
                    if self.data_list[i]["seq"] <= next_seq - 1:
                        self.data_list.pop(i)
                        # Check if the packet is in packets in flight
                        if i in self.packets_in_flight.keys():
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
                                self.info["RTO"] = []
                                self.info["SRTT"] = []
                            else:
                                alpha = 0.125
                                beta = 0.25
                                self.devrtt = (1 - beta) * self.devrtt + beta * abs(rtt - self.srtt)
                                self.srtt = (1 - alpha) * self.srtt + alpha * rtt
                                self.rto = self.srtt + max(0.010, 4 * self.devrtt)
                                self.rto = max(self.rto, 1)  # Always round up RTO.
                                self.rto = min(self.rto, 60)  # Maximum value 60 seconds.
                                self.info["RTO"].append(self.rto)
                                self.info["SRTT"].append(self.srtt)

                    # Record duplicate ack
                    elif self.data_list[i]["seq"] == next_seq:
                        self.data_list[i]["duplicate acks"] += 1
                        break
            if len(self.data_list) == 0:
                print("finish")
                self.state_history = FIN
                self.state = FIN

        if self.state != FIN and self.packets_in_flight != {}:
            # Situation of losing packets
            for key in list(self.packets_in_flight.keys()):
                if t.time() - self.packets_in_flight[key]["time"] >= self.rto:  # For test
                    self.packets_retransmit[key] = ""
                    self.packets_in_flight.pop(key)
                    if t.time() - self.time_last_lost > self.srtt:
                        self.ssthresh = 1 / 2 * self.cwnd
                        self.cwnd = 3
                        self.time_last_lost = t.time()
                else:
                    break

    # Receive result from server and four waves
    def disconnect(self):
        connection_trails_count = 0
        while True:
            # First wave
            msg = "FIN" + delimiter + str(1)
            self.send_packet(IS_FIN, msg, self.server_address, -1, -1)
            try:
                ack, address = self.sock.recvfrom(size)
            except socket.timeout:
                continue
            pkt = Packet(0, 0, 0, 0, 0, 0, ack)
            pkt.decode_seq()
            # Second wave
            if not (pkt.msg.split(delimiter)[0] == "FIN ACK" and int(pkt.msg.split(delimiter)[1]) == 1
                    and pkt.msg.split(delimiter)[2] == "ack number" and int(pkt.msg.split(delimiter)[3]) == self.seq):
                continue
            else:
                break

        # Receive final result from server
        while True:
            try:
                self.sock.settimeout(120)
                result, address = self.sock.recvfrom(size)
            except socket.timeout:
                self.state = CLOSED
                print("No such task! Do not receive final result...")
                break

            pkt = Packet(0, 0, 0, 0, 0, 0, result)
            pkt.decode_seq()
            result = pkt.msg
            if pkt.flag == IS_DATA and delimiter not in result:
                print("Final result is %s" % result.split("  ")[0])
                msg = "Result ACK" + delimiter + str(pkt.seq + 1)
                self.send_packet(IS_ACK, msg, self.server_address, self.seq, -1)
                try:
                    self.sock.settimeout(10)
                    fin, address = self.sock.recvfrom(size)
                except socket.timeout:
                    print("close socket")
                    break
                pkt = Packet(0, 0, 0, 0, 0, 0, fin)
                pkt.decode_seq()
                # Third wave
                if pkt.msg.split(delimiter)[0] == "FIN" and int(pkt.msg.split(delimiter)[1]) == 1 \
                        and pkt.msg.split(delimiter)[2] == "ACK" and int(pkt.msg.split(delimiter)[3]) == 1 \
                        and pkt.msg.split(delimiter)[4] == "ack number" and int(pkt.msg.split(delimiter)[5]) + 1 == self.seq:
                    msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(pkt.seq + 1)
                    # Fourth wave
                    self.send_packet(IS_FIN, msg, address, -1, -1)
                    t.sleep(0.002)
                    self.sock.close()
                    print("close socket")
                    break
            else:
                continue

    def run(self):
        print(t.time())
        # Connection initiation
        self.backup(self.info_file, {})
        while True:
            if self.state == CLOSED:
                logging.info("Handshaking...")
                self.handshake()
                # userInput = "1 minimum test1.txt"
                userInput = "1 average test2.txt 0.1"
                # userInput = input("\nInput file and Calculation type: ")
                self.job_id = int(userInput.split(" ")[0])
                self.cal_type = userInput.split(" ")[1]
                self.file = userInput.split(" ")[2]
                if self.cal_type == "average":
                    self.weight = userInput.split(" ")[3]

                # print("Requesting the %s in file %s" % (userInput.split(" ")[1], userInput.split(" ")[2]))
                print("Requesting the %s in file %s" % (self.cal_type, self.file))
                self.send_basic_info()
                for i in self.data_list:
                    self.data_list[i]["seq"] = self.seq
                    self.seq += 1
            elif self.state == LISTEN:
                pass
            elif self.state == CONNECTED:
                self.send_data()
                self.receive_ack()
            elif self.state == FIN:
                self.disconnect()
                self.backup(self.info_file, self.info)
                break


if __name__ == '__main__':
    client = Client2()
    client.run()
