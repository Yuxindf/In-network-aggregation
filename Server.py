import socket
import threading
import hashlib
import time
import os

serverAddress = "localhost"
serverPort = 10000

data_list = []

# Delimiter
delimiter = "|*|*|"
space = "|#|#|"
size = 200
data_list = []
client_list = []

# Receive result from server
def receive():
    result = ""
    try:
        # sock.sendto(("packet num: " + delimiter + str(len(data_list))).encode(), address)
        result, address = sock.recvfrom(size)
    except:
        print("wrong")
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
            sock.sendto(str(ave).encode(), addr)
    except:
        print("wrong")





# Start - Connection initiation
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print("Starting up on %s port %s" % (serverAddress, serverPort))
sock.bind((serverAddress, serverPort))  # Bind the socket to the port

# Listening for requests indefinitely
while True:
    print("\nWaiting to receive message")
    receive()
    if len(data_list) > 1:
        calculate()
    # connectionThread = threading.Thread(target=handle_connection)
    # connectionThread.start()
