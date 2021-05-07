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
data = load_file("./EfficiencyTest1-2.txt")
data = collections.OrderedDict(data)
labels = data["labels"]
server = data["without proxy"]
proxy = data["with proxy"]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, server, width, label='Without Proxy')
rects2 = ax.bar(x + width/2, proxy, width, label='With Proxy')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Number of Packets of Each Client')
ax.set_ylabel('Time(s)')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(True)

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.show()
