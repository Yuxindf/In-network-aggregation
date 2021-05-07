# Client id: 3
# Data does not go through proxy

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
# Test using Mininet
# serverAddress = '10.0.0.1'
# serverPort = 25139

# Delimiter
delimiter = "|*|*|"

size = 1000

# State flags
CLOSED = 1
CONNECTED = 2
FIN = 3

# Packet type flags
IS_SYN = 1
IS_INFO = 2
IS_DATA = 3
IS_ACK = 4
IS_FIN = 5

# logging
logging.basicConfig(format='[%(asctime)s.%(msecs)03d] CLIENT - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S', filename='network.log', level=logging.INFO)


class Client3:
    def __init__(self):
        self.time = t.time()  # For test
        self.client_id = 3
        # Client Initial State
        self.job_id = 0  # To identify job
        self.seq = random.randrange(1024)  # Initial sequence number(ISN) should be random
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.state_history = CLOSED  # Record the last state
        self.state = CLOSED  # Record state

        self.file = 0
        self.cal_type = 0  # Calculation type
        self.weight = -1  # Used for weighted average
        self.packet_number = 0  # The number of data that is used to calculate

        # Server and Proxy address
        self.server_address = (serverAddress, serverPort)

        # Congestion control
        self.cwnd = 3  # initial congestion window size
        self.ssthresh = 1000  # slow start threshold
        self.packet_index = 0  # Current packet index
        self.number_in_flight = 0  # Number of packets in flight
        self.packets_in_flight = collections.OrderedDict()  # Store packets that are sent bot not received ACK
        self.packets_retransmit = collections.OrderedDict()  # Store packets that should be retransmit

        self.srtt = -1  # Smooth round-trip timeout
        self.devrtt = 0  # calculate the devision of srtt and real rtt
        self.rto = 10  # Retransmission timeout
        self.time_last_lost = 0  # Time of packet loss last time
        self.last_retransmit = -1  # Sequence number of packet that last retransmitted

        # Flow control
        self.rwnd = 1000

        self.data_list = collections.OrderedDict()

    # Backup
    def backup(self, file, data):
        with open(file, "w") as f:
            store = json.dumps(data)
            f.write(store)
        f.close()

    # Load file from file system
    def load_file(self, file):
        with open(file, "r") as f:
            lines = f.read()
        f.close()
        lines = json.loads(lines)
        return lines

    # Send packet
    def send_packet(self, flag, msg, address, seq, index):
        if seq == -1:
            pkt = Packet(flag, self.job_id, self.client_id, self.seq, index, msg, 0)
            self.seq += 1
        else:
            pkt = Packet(flag, self.job_id, self.client_id, seq, index, msg, 0)
        pkt.encode_buf()
        try:
            self.sock.sendto(pkt.buf, address)
        except:
            logging.error("Fail to send packet")
        return pkt

    # Three-way handshake
    def handshake(self):
        connection_trails_count = 0
        while True:
            print("Connect with Server " + str(serverAddress) + " " + str(serverPort))
            # first handshake
            syn = 1
            # try:
            msg = "SYN" + delimiter + str(syn)
            try:
                pkt = self.send_packet(IS_SYN, msg, self.server_address, self.seq, -1)
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
            from_server.decode_buf()
            # Ensure server has received SYN packet
            if from_server.flag == IS_SYN and from_server.msg.split(delimiter)[0] == "ack number" \
                    and int(from_server.msg.split(delimiter)[1]) == pkt.seq + 1 \
                    and from_server.msg.split(delimiter)[2] == "SYN" and int(from_server.msg.split(delimiter)[3]) == 1\
                    and from_server.msg.split(delimiter)[4] == "ACK" and int(from_server.msg.split(delimiter)[5]) == 1:
                # Third handshake
                msg = "SYN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(from_server.seq + 1)
                self.send_packet(IS_SYN, msg, address, self.seq, -1)
                if self.state_history == CLOSED:
                    self.state_history = CONNECTED
                self.state = self.state_history
                print("Successfully connected")
                break

    # Receive a task from server
    def receive_task(self):
        task = ""
        try:
            task, address = self.sock.recvfrom(size)
        except:
            logging.error("Client cannot receive task from server")
        task = Packet(0, 0, 0, 0, 0, 0, task)
        task.decode_buf()
        self.job_id = int(task.msg.split(delimiter)[0])
        self.cal_type = task.msg.split(delimiter)[1].split(" ")[0]
        print("Task id %s: Calculation type is %s " % (task.msg.split(delimiter)[0], task.msg.split(delimiter)[1]))

    # Open file and add data into data_list
    def open_file(self):
        try:
            file_read = open(self.file, 'r')
            print("Opening file %s" % self.file)
            data = file_read.read()
            index = 0
            # Store all the data into a dictionary
            for i in data.split(" "):
                self.data_list[index] = {"data": i, "duplicate acks": 0}
                index += 1
            file_read.close()
        except FileNotFoundError:
            print("Requested file could not be found")

    # Send basic information to server and Receive ACK from server
    def send_basic_info(self):
        while True:
            self.open_file()
            self.packet_number = len(self.data_list)
            # Send basic information to server
            # msg will include operation type, data size...
            if self.weight == -1:
                msg = "Connect directly: info" + delimiter + str(self.packet_number)
            else:
                msg = "Connect directly: info" + delimiter + str(self.packet_number) + delimiter + str(self.weight)
            pkt = self.send_packet(IS_INFO, msg, self.server_address, -1, -1)
            try:
                self.sock.settimeout(10)
                # Receive ACK from server
                buf, address = self.sock.recvfrom(size)
            except socket.timeout:
                print("The connection is closed. Start connecting again...\n")
                self.state = CLOSED
                break
            ack = Packet(0, 0, 0, 0, 0, 0, buf)
            ack.decode_buf()
            if int(ack.msg) == pkt.seq + 1:
                break
            else:
                continue

    # Send packets to server
    def send_data(self):
        print("\nindex %s cwnd %s rwnd%s Packets in flight %s %s rto %s" %
              (self.packet_index, self.cwnd, self.rwnd, len(self.packets_in_flight),self.number_in_flight, self.rto))
        # Send packets
        if min(self.cwnd, self.rwnd) > self.number_in_flight and self.data_list != {}:
            while min(self.cwnd, self.rwnd) > self.number_in_flight:
                # Retransmit: Three duplicate acks.
                for key in self.data_list.keys():
                    if self.data_list[key]["duplicate acks"] >= 3:
                        msg = "data" + delimiter + str(self.data_list[key]["data"])
                        self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                        self.number_in_flight += 1
                        self.send_packet(IS_DATA, msg, self.server_address, self.data_list[key]["seq"], key)
                        self.data_list[key]["duplicate acks"] = 0
                        # If the last packet loss is solved
                        # and the interval between two packets loss is large enough
                        # Decrease CWND and SSTHRESH
                        if key != self.last_retransmit and t.time() - self.time_last_lost > self.srtt:
                            self.cwnd /= 2
                            self.ssthresh /= 2
                            self.last_retransmit = key
                            self.time_last_lost = t.time()
                            print("\n duplicate Current cwnd %s rwnd%s Packets in flight %s" % (self.cwnd, self.rwnd, len(self.packets_in_flight)))
                    break
                if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                    break

                # Retransmit: timeout
                if self.packets_retransmit != {}:
                    # Send data that are in retransmit dictionary
                    for key in list(self.packets_retransmit.keys()):
                        if key in self.data_list:
                            msg = "data" + delimiter + str(self.data_list[key]["data"])
                            self.number_in_flight += 1
                            self.send_packet(IS_DATA, msg, self.server_address, self.data_list[key]["seq"], key)
                            self.packets_in_flight[key] = {"seq": self.data_list[key]["seq"], "time": t.time()}
                            self.packets_retransmit.pop(key)
                            print("Retransmit: packet index %s" % key)
                            if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                                break

                if min(self.cwnd, self.rwnd) <= self.number_in_flight:
                    break

                # Send data in a normal situation,
                # i.e. send data of data list that has not been in packets in flight
                if self.packet_index < self.packet_number:
                    self.packets_in_flight[self.packet_index] = {"seq": self.data_list[self.packet_index]["seq"], "time": t.time()}
                    self.number_in_flight += 1
                    msg = "data" + delimiter + str(self.data_list[self.packet_index]["data"])
                    self.send_packet(IS_DATA, msg, self.server_address, self.data_list[self.packet_index]["seq"], self.packet_index)
                    self.packet_index += 1
                else:
                    break

        # If rwnd is 0, send empty packet to ask rwnd
        elif self.rwnd <= 0:
            t.sleep(0.1)
            self.send_packet(IS_DATA, "data", self.server_address, self.seq, -1)
            print("\nCurrent cwnd %s %s Packets in flight %s" % (self.cwnd, self.rwnd, len(self.packets_in_flight)))

    # Receive ack from server
    def receive_ack(self):
        ack = ""
        # Receive ack from server
        try:
            self.sock.settimeout(max(2, self.srtt))
            ack, address = self.sock.recvfrom(size)
        # Packet loss or ACK loss, ack for rwnd from server
        except socket.timeout:
            self.number_in_flight = 0
            self.send_packet(IS_DATA, "data", self.server_address, self.seq, -1)
            logging.info("The client does not receive ack from server")
        if ack != "":
            pkt = Packet(0, 0, 0, 0, 0, 0, ack)
            pkt.decode_buf()
            self.number_in_flight -= 1
            # Slow start
            if self.cwnd < self.ssthresh:
                self.cwnd += 1
            # Congestion control
            else:
                self.cwnd += 1.0 / self.cwnd
            # Receive ack of data
            next_seq = int(pkt.msg.split(delimiter)[0])
            self.rwnd = int(pkt.msg.split(delimiter)[1])
            if len(self.data_list) != 0:
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
                            # srtt, Jacobson / Karels algorithm to modify rto and srtt
                            if self.srtt < 0:
                                # First time setting srtt
                                self.srtt = rtt
                                self.devrtt = rtt / 2
                                self.rto = self.srtt + 4 * self.devrtt
                            else:
                                alpha = 0.125
                                beta = 0.25
                                self.srtt = self.srtt + alpha * (rtt - self.srtt)
                                self.devrtt = (1 - beta) * self.devrtt + beta * abs(rtt - self.srtt)
                                self.rto = self.srtt + 4 * self.devrtt
                                self.rto = max(self.rto, 1)  # Always round up RTO.
                                self.rto = min(self.rto, 60)  # Maximum value 60 seconds.
                                # For test, which will be drawn in bar chart or line chart

                    # Record duplicate ack
                    elif self.data_list[i]["seq"] == next_seq:
                        self.data_list[i]["duplicate acks"] += 1
                        break
            # When sending all packets and receiving acks, send FIN to server
            if len(self.data_list) == 0:
                print("finish sending")
                self.state_history = FIN
                self.state = FIN

        if self.state != FIN and self.packets_in_flight != {}:
            # Situation of losing packets: timeout
            for key in list(self.packets_in_flight.keys()):
                if t.time() - self.packets_in_flight[key]["time"] >= self.rto:
                    self.packets_retransmit[key] = ""
                    self.packets_in_flight.pop(key)
                    # If the interval between two packets loss is large enough
                    # Decrease CWND and SSTHRESH
                    if t.time() - self.time_last_lost > self.srtt:
                        self.ssthresh = 1 / 2 * self.cwnd
                        self.cwnd = 3
                        self.time_last_lost = t.time()
                else:
                    break

    # Receive result from server and four-way handshake
    def disconnect(self):
        while True:
            # First wave
            msg = "FIN" + delimiter + str(1)
            self.send_packet(IS_FIN, msg, self.server_address, self.seq, -1)
            ack, address = self.sock.recvfrom(size)
            pkt = Packet(0, 0, 0, 0, 0, 0, ack)
            pkt.decode_buf()
            # Second handshake
            if pkt.msg.split(delimiter)[0] == "FIN ACK" and int(pkt.msg.split(delimiter)[1]) == 1 \
                    and pkt.msg.split(delimiter)[2] == "ack number" and int(pkt.msg.split(delimiter)[3]) == self.seq + 1:
                break
            else:
                continue

        # Receive final result from server
        while True:
            try:
                self.sock.settimeout(360)
                result, address = self.sock.recvfrom(size)
            except socket.timeout:
                self.state = CLOSED
                print("No such task! Do not receive final result...")
                break

            pkt = Packet(0, 0, 0, 0, 0, 0, result)
            pkt.decode_buf()
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
                pkt.decode_buf()
                # Third wave
                if pkt.msg.split(delimiter)[0] == "FIN" and int(pkt.msg.split(delimiter)[1]) == 1 \
                        and pkt.msg.split(delimiter)[2] == "ACK" and int(pkt.msg.split(delimiter)[3]) == 1 \
                        and pkt.msg.split(delimiter)[4] == "ack number" and int(pkt.msg.split(delimiter)[5]) == self.seq:
                    msg = "FIN ACK" + delimiter + str(1) + delimiter + "ack number" + delimiter + str(pkt.seq + 1)
                    # Fourth wave
                    self.send_packet(IS_FIN, msg, address, self.seq, -1)
                    t.sleep(0.002)
                    self.sock.close()
                    print("close socket")
                    break
            else:
                continue

    def run(self):
        # Connection initiation
        while True:
            if self.state == CLOSED:
                logging.info("Handshaking...")
                self.handshake()
                self.receive_task()
                # Minimum or Maximum
                # userInput = "test1.txt"
                # Average
                # userInput = "test2.txt 0.1"  # The number is weight
                if self.cal_type == "average":
                    userInput = input("\nInput file and weight: ")
                else:
                    userInput = input("\nInput file:")
                self.file = userInput.split(" ")[0]
                if self.cal_type == "average":
                    self.weight = userInput.split(" ")[1]

                print("Requesting the %s in file %s" % (self.cal_type, self.file))
                self.send_basic_info()
                for i in self.data_list:
                    self.data_list[i]["seq"] = self.seq
                    self.seq += 1
            elif self.state == CONNECTED:
                self.send_data()
                self.receive_ack()
            elif self.state == FIN:
                self.disconnect()
                print("Total Time %s" % (t.time() - self.time))
                break


if __name__ == '__main__':
    client = Client3()
    client.run()
