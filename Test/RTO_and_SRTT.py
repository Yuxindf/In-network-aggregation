import collections

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import json


def load_file(file):
    with open(file, "r") as f:
        lines = f.read()
    f.close()
    lines = json.loads(lines)
    return lines


# Proxy Efficiency
# data = load_file("./EfficiencyTest.txt")
# data = collections.OrderedDict(data)
# labels = data["labels"]
# server = data["only server"]
# proxy = data["add proxy"]
#
# x = np.arange(len(labels))  # the label locations
# width = 0.35  # the width of the bars
#
# fig, ax = plt.subplots()
# rects1 = ax.bar(x - width/2, server, width, label='Only Server')
# rects2 = ax.bar(x + width/2, proxy, width, label='Add Proxy')
#
# # Add some text for labels, title and custom x-axis tick labels, etc.
# ax.set_xlabel('Packet number')
# ax.set_ylabel('Time(s)')
# ax.set_title('Efficiency of Proxy')
# ax.set_xticks(x)
# ax.set_xticklabels(labels)
# ax.legend()
# ax.grid(True)
#
# ax.bar_label(rects1, padding=3)
# ax.bar_label(rects2, padding=3)
#
# fig.tight_layout()
#
# plt.show()


# The value of RTO
# data = load_file("./client/info.txt")
# data = collections.OrderedDict(data)
# rto = data["RTO"]
#
# x = np.arange(len(rto))
# fig, ax = plt.subplots()
# ax.plot(x, rto, label="RTO")
# ax.set_xlabel('Acknowledgement Number')
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
plt.legend()

plt.show()