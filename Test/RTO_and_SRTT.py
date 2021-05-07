# Test congestion control
# Packets of one client go through proxy
# Draw CWND, RTO and SRTT

import collections

import matplotlib.pyplot as plt
import numpy as np
import json


def load_file(file):
    with open(file, "r") as f:
        lines = f.read()
    f.close()
    lines = json.loads(lines)
    return lines


# The value of cwnd and SSTHRESH
# data = load_file("../client/info.txt")
# data = collections.OrderedDict(data)
# cwnd = data["cwnd"]
# ssthresh = data["ssthresh"]
#
# x = np.arange(len(cwnd))
# fig, ax = plt.subplots()
# ax.plot(x, cwnd, label="CWND")
# ax.plot(x, ssthresh, label="SSTHRESH", linestyle="--", color="r")
# ax.set_xlabel('Acknowledgement Number')
# ax.set_ylabel('Time(s)')
# plt.legend()
#
# plt.show()


# The value of RTO
# data = load_file("../client/info.txt")
# data = collections.OrderedDict(data)
# rto = data["RTO"]
#
# x = np.arange(len(rto))
# fig, ax = plt.subplots()
# ax.plot(x, rto, label="RTO")
# ax.set_xlabel('Acknowledgement Number')
# ax.set_ylabel('Time(s)')
# plt.legend()
#
# plt.show()

# The value of SRTT
data = load_file("../client/info.txt")
data = collections.OrderedDict(data)
srtt = data["SRTT"]

x = np.arange(len(srtt))
fig, ax = plt.subplots()
ax.plot(x, srtt, label="SRTT")
ax.set_xlabel('Acknowledgement Number')
ax.set_ylabel('Time(s)')
plt.legend()

plt.show()