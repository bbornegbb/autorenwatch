import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas

types = filter (None, (row['type'] for row in tagesschau.select('type').distinct().collect()) )

groupby = "type year hour".split()

dates = spark.createDataFrame([ (t, y, h) for y in range(2000, 2014) for h in range (24) for t in types],
                              groupby)

data = spark.sql ("""select type, year(date) as year, hour(date) as hour, length(text) as len
                            from tagesschau
                            where 2000 < year(date) and year(date) < 2014""")\
            .join (dates, groupby, "right")\
            .toPandas()

dowgroupby = "type dow hour".split()

dowdates = spark.createDataFrame([ (t, dow, h) for dow in "Mon Tue Wed Thu Fri Sat Sun".split()
                                   for h in range (24) for t in types],
                                 dowgroupby)

dowdata = spark.sql ("""select type, date_format (date, 'E') as dow, hour(date) as hour, length(text) as len
                               from tagesschau
                               where 2000 < year(date) and year(date) < 2014""")\
               .join (dowdates, dowgroupby, "right")\
               .toPandas()

dowdata['dow'] = pandas.Categorical (dowdata['dow'], "Mon Tue Wed Thu Fri Sat Sun".split(), ordered=True)

colors = mpl.colors.cnames.keys()

def do_plot (d, title, output):
    dim1, dim2 = d.index.levels
    name1, name2 = d.index.names
    bottom = np.zeros(len(dim2))
    fig = plt.figure (figsize=(7,10))
    for i, d1 in enumerate (dim1):
        b = plt.bar (d.ix[d1].index-0.4, d.ix[d1].len, color=colors[i], bottom=bottom)
        b.set_label(d1)
        bottom += d.ix[d1].len.fillna(0)

    plt.title (title)
    plt.xlim (min(d.ix[d1].index)-1, max(d.ix[d1].index)+1)
    plt.ylim (ymax=1.5*plt.ylim()[1])
    plt.legend (loc="upper center", ncol=4)
    plt.xlabel (name2.capitalize())
    plt.ylabel ("Length")
    plt.savefig (output)
    plt.close()

def do_plots(quantiles, titlepattern, outputpattern):
    for t in types:
        do_plot (quantiles.ix[t], titlepattern % t, outputpattern % t)

hourgroups = data.groupby (('type', 'year', 'hour'))
yeargroups = data.groupby (('type', 'hour', 'year'))
dowgroups = dowdata.groupby (('type', 'hour', 'dow'))

quantile_setup = ((0.25, "First quarter"),
                  (0.5, "Median"),
                  (0.75, "Third quarter")
)

def legend_colorbars (ax, x, y, width, height, txt, data, colormap):
    mi = str(np.nanmin (data))
    ma = str(np.nanmax (data))
    ax.text (x+width*0.5, y+height*0.97, txt, va="top", ha="center", color="black")
    ax.text (x+width*0.03, y+height*0.03, mi, va="bottom", ha="left", color="white")
    ax.text (x+width*0.97, y+height*0.03, ma, va="bottom", ha="right", color="white")
    bbox = mpl.transforms.Bbox (((x,y),(x+width,y+height)))
    tbbox = mpl.transforms.TransformedBbox (bbox, ax.transData)
    bbox_image = mpl.image.BboxImage(tbbox, #txt.get_window_extent,
                                     norm=None,
                                     origin=None, clip_on=False,
                                     cmap=colormap)
    r = np.arange(256)
    r = np.vstack((r,r))
    bbox_image.set_data(r)
    ax.add_artist(bbox_image)


import heatmap
def do_heatmap (d, title, output, cnts, years, d_label="Size", cnt_label="Count"):
    data = np.array ([ d.ix[i].values for i in d.index.levels[0] ])
    data = data.squeeze()
    x_cnts = map (sum, zip (*cnts))
    y_cnts = map (sum, cnts)
    fig, ((ax1, ax3), (ax2, ax4)) = plt.subplots (2, 2, figsize = (7.4, 5),
                                                  sharex='col', sharey='row',
                                                  gridspec_kw={'left': 0.17,
                                                               'right': 0.95,
                                                               'height_ratios': (3, 5),
                                                               'width_ratios': (6, 3),
                                                               'hspace': 0.1,
                                                               'wspace': 0.08}
    )
    # Replace the ax3 object having shared axis' with a new plot with
    # the same dimensions
    ax3.axis('off')
    ax3 = fig.add_subplot (222, position=ax3.get_position())
    ax2.set_aspect ("equal")
    ax1.bar (left=range(24), height=x_cnts, align='center')
    ax1.set_ylabel (cnt_label)
    ax1.set_title (title)
    ax4.barh (range(len(y_cnts)), y_cnts, align='center')
    ax4.set_xlabel (cnt_label)
    heatmap.heatmap (ax2, data, "coolwarm")
    heatmap.heatmap (ax2, cnts.transpose(), 'BrBG', circles=True,
                     fill=False, nanmark=False, alpha=1, shadow=False)
    xn = len (d.index.levels[0])
    yn = len (d.index.levels[1])
    ax2.set_xlim (-1, xn)
    ax2.set_ylim (-0.5, yn + 1)
    ax2.set_xlabel (d.index.levels[0].name.capitalize())
    ax2.set_ylabel (d.index.levels[1].name.capitalize())
    yticklabels = d.index.levels[1]
    stride = (len(yticklabels)+6)/7
    yticks = range(len(yticklabels))[::stride]
    yticklabels = yticklabels[::stride]
    ax2.set_yticks (yticks)
    ax2.set_yticklabels (yticklabels)
    xt4 = ax4.get_xticks()
    stride = (len(xt4)+2) / 3
    xt4 = [int(i) for i in ax4.get_xticks()[::stride]]
    ax4.set_xticklabels (xt4)
    ax4.set_xticks (xt4)
    ax3.axis('off')
    ax3.set_aspect("equal")
    heatmap.square(ax3, 1, 7, 0.8, color="black")
    heatmap.circle(ax3, 1, 4, 0.8, edgecolor="black", fill=False, shadow=False)
    heatmap.cross(ax3, 1, 1, 0.5, color="black")
    legend_colorbars (ax3, 2, 5.6, 12, 2.8, d_label, data, "coolwarm")
    legend_colorbars (ax3, 2, 2.6, 12, 2.8, cnt_label, cnts, "BrBG")
    ax3.text (2, 1, "NA: Did't happen", va="center")
    ax3.set_xlim (0, 15)
    ax3.autoscale_view()
    plt.savefig (output)
    plt.close()

def do_heatmaps (quantiles, origindata, titlepattern, outputpattern):
    if len (quantiles.index.levels) != 3:
        raise ValueError ("Data in wrong format: need a three level index")
    data_ = origindata.dropna()
    fdim, xdim, ydim = quantiles.index.levels
    #for t in ['video']:
    for t in fdim:
        subdata = data_.query('%s == %r' % (fdim.name, t))
        cnts = np.array ([[len(subdata.query('%s == %r and %s == %r' % (xdim.name, x, ydim.name, y)))
                           for x in xdim]
                          for y in ydim])
        do_heatmap (quantiles.ix[t], titlepattern % t, outputpattern % t, cnts, ydim)


try:
    quantile_cache.keys()
except:
    quantile_cache = {}

def get_quantiles (dataset, q):
    global quantile_cache
    dckey = (dataset, q)
    if not dckey in quantile_cache:
        g = globals()[dataset]
        quantile_cache[dckey] = g.quantile(q)
    return quantile_cache[dckey]

for q, name in quantile_setup:
    fname = name.split()[0].lower()
    do_plots (get_quantiles ('yeargroups', q),
              "%s by year: %%s" % name, "/tmp/year_length_%s-%%s.pdf" % fname)
    do_plots (get_quantiles ('hourgroups', q),
              "%s by time of day: %%s" % name, "/tmp/tod_length_%s-%%s.pdf" % fname)
    do_heatmaps (get_quantiles ('yeargroups', q), data,
                 "%s size and count by hour\nand year: %%s" % name, "/tmp/hm_%s-%%s.pdf" % fname)
    do_heatmaps (get_quantiles ('dowgroups', q), dowdata,
                 "%s size and count by hour\nand day of week: %%s" % name, "/tmp/dow_%s-%%s.pdf" % fname)
