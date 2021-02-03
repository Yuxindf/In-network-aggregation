import socket
import threading
import hashlib
import time
import os
import random

serverAddress = "localhost"
serverPort = 10000

data_list = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200
data_list = []
client_list = []


# Three-way handshakes
def handshake():
    connection_trails_count = 0
    while 1:
        # Second handshake
        try:
            recv, address = sock.recvfrom(size)
            print("Connect with client " + str(address))
        except:
            connection_trails_count += 1
            if connection_trails_count < 5:
                print("\nConnection time out, retrying")
                continue
            else:
                print("\nMaximum connection trails reached, skipping request\n")
                return False
        from_client = recv.decode()
        if from_client.split(delimiter)[0] == "syn" and int(from_client.split(delimiter)[1]) == 1 \
                and from_client.split(delimiter)[2] == "seq":
            ack_number = int(from_client.split(delimiter)[3]) + 1
            syn = 1
            ack = 1
            seq = random.randrange(0, 10000, 1)
            try:
                sock.sendto(("ack number" + delimiter + str(ack_number) + delimiter + "syn" + delimiter + str(syn) \
                             +delimiter+ "ack" + delimiter + str(ack) + delimiter + "seq" + delimiter + str(seq)) \
                            .encode(), address)
            except:
                print("Internal Server Error")
        try:
            recv, address = sock.recvfrom(size)
        except:
            print("Internal Server Error")
        from_client = recv.decode()

        # Third handshake
        if from_client.split(delimiter)[0] == "seq" and int(from_client.split(delimiter)[1]) == seq + 1 \
                and from_client.split(delimiter)[2] == "ack" and int(from_client.split(delimiter)[3]) == 1:
            return True


# Receive result from proxy
def receive_from_proxy():
    result = ""
    try:
        # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
        result, address = sock.recvfrom(size)
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
def calculate():
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
            sock.sendto(("ave is " + str(ave)).encode(), addr)
    except:
        print("Internal Server Error")


# Start - Connection initiation
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print("Starting up on %s port %s" % (serverAddress, serverPort))
sock.bind((serverAddress, serverPort))  # Bind the socket to the port

# Listening for requests indefinitely
while True:
    print("\nWaiting to receive message")
    if not handshake():
        break
    receive_from_proxy()
    if len(data_list) > 1:
        calculate()
    # connectionThread = threading.Thread(target=handle_connection)
    # connectionThread.start()
