# create animation of daily clusters bar charts
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation

filename = "s3a://edgarlogoutput/k10minDF750.csv"
data = pd.read_csv(filename)

grouped = data.groupby(["date"])

# for i, (date, clusters) in enumerate(grouped):
#     preds = clusters["prediction"].value_counts()
#     plt.bar(
#         preds.index,
#         preds.values,
#         label=date,
#     )
#     plt.legend()
#     plt.yscale("log")
#     plt.show()
#     if i == 2:
#         break

fig, ax = plt.subplots()
ax.bar([], [])

def init():
    ax.set_yscale("log")
    ax.set_ylabel("Occupancy")
    ax.set_xlabel("Cluster")
    ax.set_xlim(-1, 10)
    ax.set_xticks(np.arange(10))
    ax.set_xticklabels(np.arange(10))

def update(i):
    for bar in ax.containers:
        bar.remove()
    date, clusters = list(grouped)[i]
    preds = clusters["prediction"].value_counts()
    ax.bar(
        preds.index,
        preds.values,
        color="blue",
    )
    ax.set_title(f"Cluster occupancies for {date}")

anim = animation.FuncAnimation(
    fig=fig,
    func=update,
    init_func=init,
    frames=len(grouped),
    interval=50,
    repeat=False,
)

# Writer = animation.writers['ffmpeg']
# writer = animation.ImageMagickWriter()
# writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)
# anim.save("daily_occupancies.mp4", writer=writer)
plt.show()
