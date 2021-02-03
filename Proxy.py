# udp server
import socket
import hashlib
import os

host = 'localhost'
proxy_port = 6001  # Proxy Port
server_port = 10000  # Map to Serer Port

data_list = []
result_list = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200

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


# do some calculations
def calculate(index, client_address):
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

    return ave


def send_to_server():
    index_client = process_data()
    index = index_client[0]
    client_address = index_client[1]
    ave = calculate(index, client_address)
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
print(host + " Client connect to %s\n" % server_port)

while 1:
    print("\nWaiting to receive message")
    data, address = proxy.recvfrom(size)
    print(data)
    send_to_server()

