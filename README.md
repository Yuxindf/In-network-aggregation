# Transport Protocol for In-Network Aggregation
## Introduction
This network protocol based on UDP. A proxy is created between servers and clients to aggregate data before they arrive at server. (In practical scenarios, multiple proxy will be deployed in local).
Before clients’ packets arriving at server, they will go through proxy, in which the packets of the same job in the same index will be aggregated. To make the in-network aggregation more efficient, more steps are added to improve the logic of the project. Also, a more complex situation is added, which is that packets of client are sent to server directly without going through proxy. In addition, UDP is quick but not reliable at all, to balance the speed and reliability of the protocol, reliable transmission over UDP is implemented by imitating some mechanisms of TCP, including three-way handshake, four-way handshake, acknowledgement mechanism, retransferring mechanism, congestion control mechanism and flow control mechanism. Besides, data backup is achieved for clients, server and proxy.

## Instructions to Run the Program
### Requirements
Requirements to run the program:
 1) The development environment is Python3.7, but it can run on any version of python3
 2) Numpy library of python, this can be downloaded using command ‘pip install numpy’
### How to run the program
To run the program:
 1) Clone the repository and unzip the code.
 2) Run Server.py and Proxy.py at first.
 3) If run on windows, the StartAllNodes.bat in the root directory can be run to open all three clients. In linux, three clients should be opened manually by input "python filename".
 4) According to the task displayed in each task, input corresponding arguments. For calculation type of maximum and minimum, input "test1.txt". For calculation type of average, input "test2.txt 0.1", and the second argument is weight of the client, which can be from 0 to 1 (not equal to 0).
 5) Besides the output during the process of running program, the final output for each client is final result and total execution time.


