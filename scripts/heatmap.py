#!/usr/bin/env python

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, PathPatch, Circle
from matplotlib.path import Path
import numpy as np

def square (axes, x, y, size, fill=True, color=None, edgecolor="none", alpha=None, shadow=False):
    size2 = size / 2.
    rect = Rectangle((x-size2,y-size2), size, size, edgecolor=edgecolor,
                     facecolor=color, alpha=alpha, fill=fill)
    return axes.add_patch(rect)

def cross (axes, x, y, size, color=None, alpha=None):
    size2 = size / 2.
    #axes.plot((x-size2, x+size2), (y-size2, y+size2), color=color)
    #axes.plot((x-size2, x+size2), (y+size2, y-size2), color=color)
    pdata = [
        (Path.MOVETO, (x-size2,y-size2)),
        (Path.LINETO, (x+size2,y+size2)),
        (Path.MOVETO, (x-size2,y+size2)),
        (Path.LINETO, (x+size2,y-size2))
    ]
    codes, verts = zip (*pdata)
    path = Path(verts, codes)
    return axes.add_patch(PathPatch(path, fill=False, color=color, alpha=alpha ))

def circle (axes, x, y, size, fill=True, color=None, edgecolor="none", alpha=None, shadow=False):

    radius = size / 2.
    if shadow:
        c = Circle ((x, y), radius, fill=fill, color="white", alpha=alpha, linewidth=2)
        axes.add_patch (c)
    c = Circle ((x, y), radius, fill=fill, color=color, alpha=alpha)
    return axes.add_patch (c)

def heatmap (ax, data, colormapname, fill=True, alpha=None, circles=False, nanmark=True, shadow=False):
    shape = data.shape

    if fill:
        edgecolor="none"
    else:
        edgecolor=None

    ma = float(np.nanmax(data))
    #mi = data.min()

    #ndata = (data - mi) / (ma - mi) / 1.2
    cdata = data / ma if ma > 0 else data
    ndata = cdata**0.5 / 1.2

    cmap = plt.get_cmap (colormapname, 256)

    for x in range (shape[0]):
        for y in range (shape[1]):
            if np.isnan (ndata[x][y]):
                if nanmark:
                    cross (ax, x, y, 0.2, "black")
            elif ndata[x][y] == 0:
                pass
            else:
                if circles:
                    circle (ax, x, y, ndata[x][y], fill, cmap(cdata[x][y]), edgecolor, alpha, shadow)
                else:
                    square (ax, x, y, ndata[x][y], fill, cmap(cdata[x][y]), edgecolor, alpha, shadow)

    ax.autoscale_view()


#data = np.array ([[ 1, 2, 3, 4 ],
#                  [ 5, 2.5, 0.3, 4 ]], float).transpose()

if __name__ == "__main__":
    import cPickle as pickle
    data = pickle.load (open ("/tmp/hm.pickle"))
    data = data.squeeze()

    fig = plt.figure()
    ax = fig.add_subplot(111, aspect="equal")
    heatmap (ax, np.nanmax(data)-data+100, "coolwarm", alpha=1, circles=True, fill=False, nanmark=False)
    heatmap (ax, data, "rainbow", alpha=0.5, nanmark=False)
    plt.xlabel ("Hour")
    plt.ylabel ("Year-2000")
    plt.title ("video")
    #plt.legend()
    #plt.savefig ("/tmp/hm-median-video.pdf")
    plt.show()
    plt.close()
