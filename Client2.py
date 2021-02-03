import socket
import hashlib
import os
import time
import datetime
import random

# Set address and port
serverAddress = "127.0.0.1"
serverPort = 10000
proxyAddress = "127.0.0.1"
proxyPort = 6001

# Delimiter
delimiter = "|*|*|"

size = 200

# Packet class definition
class Packet:
    checksum = 0
    seqNo = 1
    msg = 0
    index = 2

    def make(self, data):
        self.msg = data
        # change the parameter to bytes type
        self.checksum = hashlib.sha256(data.encode()).hexdigest()
        print("Length: %s\nSequence number: %s" % (len(data.split(" ")), self.seqNo))


# Three-way handshakes
def handshake(address):
    connection_trails_count = 0
    while 1:
        print("Connect with Server " + str(serverAddress) + " " + str(serverPort))
        # first handshake
        syn = 1
        seq = random.randrange(0, 10000, 1)
        try:
            sock.sendto(("syn" + delimiter + str(syn) + delimiter+ "seq" + delimiter + str(seq)).encode(), address)
        except:
            print("Internal Server Error")
        try:
            ack, address = sock.recvfrom(size)
        except:
            connection_trails_count += 1
            if connection_trails_count < 5:
                print("\nConnection time out, retrying")
                continue
            else:
                print("\nMaximum connection trails reached, skipping request\n")
                return False
        from_server = ack.decode()
        # Third handshake
        if from_server.split(delimiter)[0] == "ack number" and int(from_server.split(delimiter)[1]) == seq + 1 \
                and from_server.split(delimiter)[2] == "syn" and int(from_server.split(delimiter)[3]) == 1 \
                and from_server.split(delimiter)[4] == "ack" and int(from_server.split(delimiter)[5]) == 1 \
                and from_server.split(delimiter)[6] == "seq":
            ack = 1
            seq = int(from_server.split(delimiter)[7]) + 1
            try:
                sock.sendto(("seq" + delimiter + str(seq) + delimiter + "ack" + delimiter+ str(ack)).encode(), address)
            except:
                print("Internal Server Error")
            return True



def open_file(file):
    try:
        file_read = open(file, 'r')
        print("Opening file %s" % file)
        data = file_read.read()
        data_list = data.split(" ")
        file_read.close()
    except:
        print("Requested file could not be found")
    return data_list


# Send basic information to server
def send_to_server():
    pkt = Packet()
    try:
        while 1:
            data_list = open_file()
            # msg will include operation type, data size...
            msg = len(data_list)
            print(msg)
            pkt.make(msg)
            packet = str(pkt.checksum) + delimiter + str(pkt.seqNo) + delimiter + str(
                pkt.index) + delimiter + pkt.msg
    except:
        print("Internal server error")


# Unpack data
def unpack(file, address):
    drop_count = 0
    packet_count = 0
    start_time = time.time()
    pkt = Packet()

    try:
        data_list = open_file(file)

        # Send packet number to proxy
        while 1:
            send_packet = sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
            sock.settimeout(1)
            try:
                ack, address = sock.recvfrom(size)
            except:
                print("Time out reached, resending...packet number")
                continue
            if ack.decode().split(delimiter)[0] == "Ack: packet num: " \
                    and ack.decode().split(delimiter)[1] == str(len(data_list)):
                break
            else:
                continue

        x = 0
        # Fragment and send data one by one
        while x < len(data_list):
            packet_count += 1
            msg = data_list[x]
            print(msg)
            pkt.make(msg)
            # pack
            final_packet = str(pkt.checksum) + delimiter + str(pkt.seqNo) + delimiter + str(
                pkt.index) + delimiter + pkt.msg

            # Send packet
            send_packet = sock.sendto(final_packet.encode(), address)
            print("Sent %s bytes to %s, wait acknowledgment.." % (send_packet, address))
            try:
                # Receive ack from server
                ack, address = sock.recvfrom(size)
            except:
                print("Time out reached, resending...package %s" % x)
                continue
            if ack.decode() == str(pkt.seqNo):
                pkt.seqNo += 1
                print("Acknowledged by: " + ack.decode() + "\nAcknowledged at: " + str(
                    datetime.datetime.utcnow()) + "\nElapsed: " + str(time.time() - start_time) + "\n")
                x += 1
        print("Packets sended: " + str(packet_count))
    except:
        print("Internal server error")


# Receive result from server
def result_from_server():
    result = ""
    try:
        # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
        sock.settimeout(4)
        result, address = sock.recvfrom(size)
    except:
        print("Internal Server Error")
    print(result.decode())


# Connection initiation
while 1:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(10)
    server_address = (serverAddress, serverPort)
    proxy_address = (proxyAddress, proxyPort)
    if not handshake(server_address):
        break
    userInput = input("\nInput file: ")
    try:
        sock.sendto("Start sending".encode(), proxy_address)
        print("Requesting the average in file %s" % userInput)
        unpack(userInput, proxy_address)
        result_from_server()

    finally:
        print("Closing socket")
        sock.close()
        break
