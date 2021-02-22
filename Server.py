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


# Receive basic information from client and send to proxy
def client_basic_info():
    # Receive basic information from client
    connection_trails_count = 0
    while 1:
        buf, address = sock.recvfrom(size)
        info = Packet(0, 0, 0, 0, 0, buf)
        info.decode_seq()
        # Send ACK to client
        pkt = Packet(0, 0, info.seq + 1, len(str(info.seq+1)), info.seq + 1, 0)
        pkt.encode_seq()
        sock.sendto(pkt.buf, address)
        cal_type = info.msg.decode().split(delimiter)[0]
        data_size = info.msg.decode().split(delimiter)[1]
        client_seq = info.seq
        print(client_seq)
        # msg will include operation type, client address, client seq and size
        msg = str(address) + delimiter + str(client_seq) + delimiter + cal_type + delimiter + data_size

        # Send basic information to proxy
        pkt = Packet(0, 0, seq, len(msg), msg, 0)
        pkt.encode_seq()
        sock.sendto(pkt.buf, proxy_address)
        try:
            ack, address = sock.recvfrom(size)
        except:
            print("Time out reached, resending...packet number")
            continue
        ack = Packet(0, 0, 0, 0, 0, ack)
        ack.decode_seq()
        if ack.seq == seq + 1:
            print("ok")
            break
        else:
            continue
    return cal_type


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
def calculate(cal_type):
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
                sock.sendto(("ave is " + str(ave)).encode(), addr)
            elif cal_type == "sum":
                sock.sendto(("sum is " + str(total)).encode(), addr)
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
    calculation_type = client_basic_info()
    receive_from_proxy()
    if len(data_list) == 1:
        calculate(calculation_type)
    # connectionThread = threading.Thread(target=handle_connection)
    # connectionThread.start()
