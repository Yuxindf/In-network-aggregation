# Test Complex situation: three clients
# (1) Target: packets of two go through proxy, one to server directly
# (2) Contrast: packets of three clients go through proxy
# Draw the time cost of waiting answer for clients

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
data = load_file("./EfficiencyTest1-3.txt")
data = collections.OrderedDict(data)
labels = data["labels"]
only_to_proxy = data["all to proxy"]
also_to_server = data["one to server"]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, only_to_proxy, width, label='All Packets go through Proxy')
rects2 = ax.bar(x + width/2, also_to_server, width, label="One Client's Packets go to Server Directly ")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Number of Packets of Each Client')
ax.set_ylabel('Time(s)')
ax.set_title('Efficiency of Two Situations', fontsize="10")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(True)

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.show()
