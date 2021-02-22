# udp server
import socket
import hashlib
import os
from Packet import Packet

host = '127.0.0.1'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port

wait_ack_list = []
data_list = []
result_list = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200


# Receive basic information of client from server and send ACK to server and client
def client_basic_info():
    connection_trails_count = 0
    while 1:
        try:
            # receive basic information from server
            info, address = proxy.recvfrom(size)
        except:
            connection_trails_count += 1
            if connection_trails_count < 5:
                print("\nConnection time out, retrying")
                continue
            else:
                print("\nMaximum connection trails reached, skipping request\n")
                return False
        info = Packet(0, 0, 0, 0, 0, info)
        info.decode_seq()
        tmp = info.msg.decode() # info
        # Send Ack to Server
        pkt = Packet(0, 0, info.seq + 1, 0, info.seq + 1, 0)
        pkt.encode_seq()
        proxy.sendto(pkt.buf, address)
        # Send Ack to Proxy
        pkt = Packet(0, 0, 0, 0, int(tmp.split(delimiter)[1]) + 1, 0)
        pkt.encode_seq()
        # obtain client address
        c = tmp.split(delimiter)[0]
        client_address = str(c[1:-1].split(", ")[0][1:-1])
        client_port = c[1:-1].split(", ")[1]
        client = (client_address, int(client_port))
        proxy.sendto(pkt.buf, client)
        print("Receive basic information of a client %s " % c)
        print(tmp.split(delimiter)[3])

    return tmp.split(space)[1]


def process_data():
    # The file is to make a backup
    f = open("new_test1.txt", 'w')
    seq_no_flag = 1
    try:
        # Receive the number of packets
        while 1:
            # Receive indefinitely
            try:
                # global client_address
                num, client_address = proxy.recvfrom(size)
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
                print("the number of packets is: " + str(packet_num))
                proxy.sendto(("Ack: " + num).encode(), client_address)
                break

        while seq_no_flag < int(packet_num) + 1:
            # Receive indefinitely
            try:
                packet, client_address = proxy.recvfrom(size)
                connection_trails_count = 0
                wait_ack_list.append(packet.decode())
                print("wait ack: " + wait_ack_list)
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
                # store in file
                f.write(str(client_address) + delimiter + packet.split(delimiter)[2]
                        + delimiter + packet.split(delimiter)[3] + space)
                # store in data list
                data_list.append(str(client_address) + delimiter + packet.split(delimiter)[2]
                                 + delimiter + packet.split(delimiter)[3])
                print("Sequence number: %s" % seq_no)
                proxy.sendto(seq_no.encode(), client_address)
            else:
                print("Checksum mismatch detected, drop packet")
                continue
        f.close()
        print("Packets served: " + str(seq_no_flag - 1))
        return index, client_address
    except:
        print("Internal server error")


# 带权平均，packet header可以带一个权重，默认为1。server那里可知，做到全局平均。
# do some calculations
def calculate(index, client_address, cal_type):
    # with open("new_test1.txt", "r") as f:
    #     for line in f:
    #         word_list = line.split(space)
    #         for a in word_list[:-1]:
    #             if int(index) == int(a.split(delimiter)[1]):
    #
    # f.close()
    total = 0
    count = 0
    for a in data_list[::-1]:
        if int(index) == int(a.split(delimiter)[1]):
            number = a.split(delimiter)[2]
            data_list.remove(a)
            count += 1
            total = total + int(number)

    ave = total / count
    print("the average is", ave)
    if cal_type == "average":
        return ave
    if cal_type == "sum":
        return total


def send_to_server(cal_type):
    index_client = process_data()
    index = index_client[0]
    client_address = index_client[1]
    ave = calculate(index, client_address, cal_type)
    packet = str(client_address) + delimiter + str(ave)
    server_address = (host, server_port)
    try:
        print("Send to server")
        proxy.sendto(packet.encode(), server_address)
    except:
        print("Internal Server Error")


proxy = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
proxy.bind((host, proxy_port))
print("Proxy start up on %s port %s\n" % (host, proxy_port))

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(host + " Client connect to Server %s\n" % server_port)

while 1:
    print("\nWaiting to receive message")
    # data, address = proxy.recvfrom(size)

    calculation_type = client_basic_info()
    send_to_server(calculation_type)

