# Test three kinds of calculations
# Packets of one client go through proxy
# Draw the time cost for three calculations

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


# The value of
data = load_file("../client/info.txt")
data = collections.OrderedDict(data)
average = data["average"]
minimum = data["minimum"]
maximum = data["maximum"]

x = np.arange(len(average))
fig, ax = plt.subplots()
ax.set_title("Comparison of Three Calculation Types", fontsize="10")
ax.plot(x, average, label="average", color="red",linestyle="--")
ax.plot(x, minimum, label="minimum")
ax.plot(x, maximum, label="maximum", color="green",linestyle=":")
ax.set_xlabel('Acknowledgement Number')
ax.set_ylabel('Time(s)')
plt.legend()

plt.show()