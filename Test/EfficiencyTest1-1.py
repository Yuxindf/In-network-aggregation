# Test the packets of data of two clients go through proxy
# And that of one client's data send directly to server

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
data = load_file("./EfficiencyTest1-1.txt")
data = collections.OrderedDict(data)
labels = data["labels"]
server = data["only server"]
proxy = data["add proxy"]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, server, width, label='Only Server')
rects2 = ax.bar(x + width/2, proxy, width, label='Add Proxy')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Packet number')
ax.set_ylabel('Number of Packets Received')
ax.set_title('Comparison of Number of Packets Server received', fontsize="10")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(True)

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.show()
