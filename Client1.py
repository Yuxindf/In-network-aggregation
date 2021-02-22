import socket
import hashlib
import os
import numpy as np
import time
import datetime
import random
from Packet import Packet

# Set address and port
serverAddress = "127.0.0.1"
serverPort = 10000
proxyAddress = "127.0.0.1"
proxyPort = 6001
receive_window_size = 24

job_id = 1
index = 1
seq = 1

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"

size = 200

# numpy库 相当于C的数组，定义数组类型，int 8型等。。。
# tobytes，数组的一个函数。得到一个序列
# 要解序列化。 frombuffer, 转化回数组。
# numpy有相应函数：max，ave等
# 运算：加权平均；取最大；取最小。差不多了。
# 公共结构、数可以放进一个class，方便改。
# 要答辩
# Packet class definition

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


# Store basic information to compare the efficiency between in-network aggregation and without it
def store_basic_info():
    return 0


# Send basic information to server and Receive ACK from proxy
def send_basic_info(file, cal_type):
    while 1:
        data_list = open_file(file)
        # msg will include operation type, data size...
        msg = cal_type + delimiter + str(len(data_list)) ### 类型编码成整数，占用32位或8位。header可以固定。变长也可。
        global seq
        pkt = Packet(job_id, index, seq, len(msg), msg, 0)
        print(seq)
        pkt.encode_seq()
        # Send basic information to server
        send_packet = sock.sendto(pkt.buf, server_address)
        try:
            # Receive ACK from server
            buf, address = sock.recvfrom(size)
        except:
            print("Time out reached, resending...")
            continue
        ack = Packet(0, 0, 0, 0, 0, buf)
        ack.decode_seq()
        if ack.seq == pkt.seq + 1:
            try:
                # Receive from proxy
                buf, address = sock.recvfrom(size)
            except:
                print("Time out reached, resending...")
                continue
            ack = Packet(0, 0, 0, 0, 0, buf)
            ack.decode_seq()
            if int(ack.msg) == pkt.seq + 1:
                print("okkkkkkkkkkk")
                break
            else:
                continue

        else:
            continue


# Unpack data
def unpack(file, address):
    window_size = 1 # real window size
    limit_window_size = 16 # window size of starting congestion avoidance algorithm
    drop_count = 0
    packet_count = 0
    start_time = time.time()

    try:
        data_list = open_file(file)

        x = 0
        # Fragment and send data one by one
        while x < len(data_list):
            count = 0
            for i in range(0, window_size):
                ack_flag = False
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

            # Quick retransmission algorithm
            for i in range(0, window_size):
                for x in range(0, 3):
                    try:
                        # Receive ack from server
                        ack, address = sock.recvfrom(size)
                    except:
                        print("Time out reached, resending...package %s" % x)
                        continue
                    if ack.decode() == str(pkt.seqNo):
                        ack_flag = True
                        pkt.seqNo += 1 # 加上一次的数据量 # congestion window同 + 包的大小/window # 两者要同！！！
                        print("Acknowledged by: " + ack.decode() + "\nAcknowledged at: " + str(
                            datetime.datetime.utcnow()) + "\nElapsed: " + str(time.time() - start_time) + "\n")
                        x += 1
                    if ack_flag:
                        if window_size < receive_window_size:
                            # Slow start algorithm
                            if window_size < limit_window_size:
                                window_size = window_size * 2
                            # Congestion avoidance algorithm
                            elif window_size >= limit_window_size:
                                window_size += 1
                        break
                    # Quick recovery algorithm
                    if x == 2:
                        window_size = window_size / 2
                        limit_window_size = max(window_size, limit_window_size / 2)
                        send_packet = sock.sendto(final_packet.encode(), address)
                        print("Sent %s bytes to %s, wait acknowledgment.." % (send_packet, address))
                        x = 0

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
    userInput = input("\nInput file and Calculation type: ")
    print("Requesting the %s in file %s" % (userInput.split(",")[1], userInput.split(",")[0]))
    if "average" in userInput.split(",")[1]:
        calculation_type = "average"
    elif "sum" in userInput.split(",")[1]:
        calculation_type = "sum"
    send_basic_info(userInput.split(",")[0], calculation_type)
    sock.sendto("Start sending".encode(), proxy_address)
    unpack(userInput.split(",")[0], proxy_address)
    result_from_server()

    # finally:
    #     print("Closing socket")
    #     sock.close()
    #     break
