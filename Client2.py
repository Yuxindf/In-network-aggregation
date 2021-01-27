import socket
import hashlib
import os
import time
import datetime

# Set address and port
serverAddress = "localhost"
serverPort = 6001

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


# Unpack data
def unpack(file, address):
    drop_count = 0
    packet_count = 0
    start_time = time.time()
    pkt = Packet()

    try:
        try:
            file_read = open(file, 'r')
            print("Opening file %s" % file)
            data = file_read.read()
            data_list = data.split(" ")
            file_read.close()
        except:
            print("Requested file could not be found")
            return

        # Send packet number to server
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
def receive():
    result = ""
    try:
        # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
        sock.settimeout(4)
        result, address = sock.recvfrom(size)
    except:
        print("wrong")
    print(result.decode())


# Connection initiation
while 1:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(10)
    server_address = (serverAddress, serverPort)
    userInput = input("\nInput file: ")
    try:
        sock.sendto("start sending".encode(), server_address)
        print("Requesting the average in file %s" % userInput)
        unpack(userInput, server_address)
        receive()

    finally:
        print("Closing socket")
        sock.close()
