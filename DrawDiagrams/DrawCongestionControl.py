# Draw diagrams to simulate congestion control. It is not real data.
# Diagrams are used in the implementation part of the final report

import matplotlib.pyplot as plt

x_ax = [0,1,2,3,4,5,6,7,8,8.3,9.3,10.3,11.3,12.3,13.3]
y_ax = [3,6,12,24,48,96,192,384,768,1000,1001,1002,1003,1004,1005]
y_ssthresh = [1000] * len(x_ax)

# Timeout
# x_ax += [13.301,14.3,15.3,16.3,17.3,18.3,19.3,20.3,20.6,21.6,22.6,23.6,24.6]
# y_ax += [3,6,12,24,48,96,192,384,503,504,505,506,507]
# y_ssthresh += [502.5] * (len(x_ax)-15)

# Three Duplicate ACKs
# x_ax += [13.301,14.3,15.3,16.3,17.3,18.3,19.3,20.3,21.3,22.3,23.3,24.3,25.3]
# y_ax += [502.5,503.5,504.5,505.5,506.5,507.5,508.5,509.5,510.5,511.5,512.5,513.5,514.5]
# y_ssthresh += [500] * (len(x_ax)-15)


fig, ax = plt.subplots()
ax.plot(x_ax, y_ssthresh, label="ssthresh", color="purple", linestyle='--')
ax.plot(x_ax, y_ax, label="window size", color="green")
ax.set_xlabel('RTT')
ax.set_ylabel('Number of Packets')
ax.legend()
ax.text(7, 700, 'Slow Start', horizontalalignment='center', verticalalignment='top', multialignment='center')
ax.text(9, 970, 'Congestion Control', verticalalignment='top', multialignment='center')
# Timeout
# ax.text(19, 300, 'Slow Start', horizontalalignment='center', verticalalignment='top', multialignment='center')
# ax.text(15, 700, 'Timeout', color='r',
#         horizontalalignment='center', verticalalignment='top', multialignment='center')
# ax.text(20, 480, 'Congestion Control', verticalalignment='top', multialignment='center')
# # Three Duplicate ACKs
# ax.text(15, 700, 'Three Duplicate ACKs', color='r',
#         horizontalalignment='center', verticalalignment='top', multialignment='center')
# ax.text(17, 480, 'Congestion Control', verticalalignment='top', multialignment='center')


for a, b in zip(x_ax, y_ax):
    if b==3 or b==1000 or b==1005 or b==503 or b==502.5 or b==514.5:
        plt.text(a, b, (a,round(b, 3)), ha='center', va='bottom', fontsize=10)


plt.show()
