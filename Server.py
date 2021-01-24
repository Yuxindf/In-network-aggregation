import socket
import threading
import hashlib
import time
import os

serverAddress = "localhost"
serverPort = 10000

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200
clientAddress = ""

def pack():
    f = open("new_test1.txt", 'a')
    seq_no_flag = 1
    try:
        # Receive the number of packets
        while 1:
            # Receive indefinitely
            try:
                global clientAddress
                num, clientAddress = sock.recvfrom(size)
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
            num = num.decode()
            packet_num = num.split(delimiter)[1]
            if num.split(delimiter)[0] == "packet num: ":
                print("the number of packets is: "+str(packet_num))
                sock.sendto(("Ack: " + num).encode(), clientAddress)
                break

        while seq_no_flag < int(packet_num) + 1:
            # Receive indefinitely
            try:
                packet, clientAddress = sock.recvfrom(size)
                connection_trails_count = 0
            except:
                connection_trails_count += 1
                if connection_trails_count < 5:
                    print("\nConnection time out, retrying")
                    continue
                else:
                    print("\nMaximum connection trails reached, skipping request\n")
                    os.remove("new_test1.txt")
                    break
            packet = packet.decode()
            seq_no = packet.split(delimiter)[1]
            index = packet.split(delimiter)[2]
            server_hash = hashlib.sha256(packet.split(delimiter)[3].encode()).hexdigest()
            print("Client hash: " + packet.split(delimiter)[0])
            print("Server hash: " + server_hash)
            if packet.split(delimiter)[0] == server_hash and seq_no_flag == int(seq_no):
                seq_no_flag += 1
                f.write(str(clientAddress) + delimiter + packet.split(delimiter)[2]
                        + delimiter + packet.split(delimiter)[3] + space)
                print("Sequence number: %s" % seq_no)
                sock.sendto(seq_no.encode(), clientAddress)
            else:
                print("Checksum mismatch detected, drop packet")
                continue
        f.close()
        print("Packets served: " + str(seq_no_flag - 1))
        return index, clientAddress
    except:
        print("Internal server error")


# do some calculations
def calculate(index, client_address):
    with open("new_test1.txt", "r") as f:
        total = 0
        count = 0
        for line in f:
            word_list = line.split(space)
            for a in word_list[:-1]:
                if int(index) == int(a.split(delimiter)[1]):
                    number = a.split(delimiter)[2]
                    count += 1
                    total = total + int(number)
            ave = total / count
            print("the average is", ave)
    f.close()
    try:
        sock.sendto(str(ave).encode(), client_address)
    except:
        print("wrong")


def handle_connection():
    index_server = pack()
    index = index_server[0]
    clientAddress = index_server[1]
    calculate(index, clientAddress)


# Start - Connection initiation
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print("Starting up on %s port %s" % (serverAddress, serverPort))
sock.bind((serverAddress, serverPort))  # Bind the socket to the port

# Listening for requests indefinitely
while True:
    print("\nWaiting to receive message")
    data, address = sock.recvfrom(size)
    handle_connection()
    # connectionThread = threading.Thread(target=handle_connection)
    # connectionThread.start()

