import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

#types = filter (None, (row['type'] for row in tagesschau.select('type').distinct().collect()) )
#
#groupby = "type year hour".split()
#
#dates = spark.createDataFrame([ (t, y, h) for y in range(2000, 2014) for h in range (24) for t in types],
#                              groupby)
#
#data = spark.sql ("""select type, year(date) as year, hour(date) as hour, length(text) as len
#                            from tagesschau
#                            where 2000 < year(date) and year(date) < 2014""")\
#            .join (dates, groupby, "right")\
#            .toPandas()

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
        do_plot (quantiles.ix[t], titlepattern % t, t, outputpattern % t)

hourgroups = data.groupby (('type', 'year', 'hour'))
yeargroups = data.groupby (('type', 'hour', 'year'))

quantile_setup = ((0.25, "First quarter"),
                  (0.5, "Median"),
                  (0.75, "Third quarter")
)

import heatmap
def do_heatmap (d, title, output, cnts, years):
    data = np.array ([ d.ix[i].values for i in d.index.levels[0] ])
    data = data.squeeze()
    hour_cnts = map (sum, zip (*cnts))
    year_cnts = map (sum, cnts)
    fig, ((ax1, ax3), (ax2, ax4)) = plt.subplots (2, 2, figsize = (7.4, 5),
                                                  gridspec_kw={'left': 0.17,
                                                               'right': 0.95,
                                                               'height_ratios': (3, 5),
                                                               'width_ratios': (6, 3),
                                                               'hspace': 0.1,
                                                               'wspace': 0.08}
    )
    ax1.bar (left=range(24), height=hour_cnts, align='center')
    ax1.set_ylabel ("Article count")
    ax1.set_title (title)
    ax1.set_xticks([])
    ax1.set_xticklabels ([])
    ax4.barh (range(len(year_cnts)), year_cnts, align='center')
    ax4.set_xlabel ("Article count")
    heatmap.heatmap (ax2, data, "coolwarm")
    heatmap.heatmap (ax2, cnts.transpose(), 'rainbow', circles=True, fill=False, nanmark=False, alpha=0.5, shadow=True)
    ax2.set_aspect ("equal")
    ax2.set_xlim (-1, 24.5)
    ax2.set_ylim (-1, 15.5)
    ax2.set_xlabel ("Hour")
    ax2.set_ylabel ("Year")
    lastyear = years.max()
    firstyear = years.min()
    yticks = np.arange(0,lastyear-firstyear,2)
    ax2.set_yticks (yticks)
    ax2.set_yticklabels (yticks + firstyear)
    ax1.set_xlim (ax2.get_xlim())
    ax4.set_ylim (ax2.get_ylim())
    ax4.set_yticks ([])
    ax4.set_yticklabels ([])
    xt4 = ax4.get_xticks()
    stride = (len(xt4)+2) / 3
    xt4 = [int(i) for i in ax4.get_xticks()[::stride]]
    ax4.set_xticklabels (xt4)
    ax4.set_xticks (xt4)
    ax3.axis('off')
    fig.savefig (output)
    plt.close()

def do_heatmaps (quantiles, titlepattern, outputpattern):
    data_ = data.dropna()
    years = [ levels for levels in quantiles.index.levels if levels.name == 'year' ][0]
    #for t in ['video']:
    for t in types:
        subdata = data_.query('type == "%s"' % t)
        cnts = np.array ([[len(subdata.query('hour == %d and year == %d' % (h, y))) for h in range(24)]
                          for y in years])
        do_heatmap (quantiles.ix[t], titlepattern % t, outputpattern % t, cnts, years)


#if not 'quantile_cache' in globals():
#    quantile_cache = {}

def get_quantiles (dataset, q):
    global quantile_cache
    dckey = (dataset, q)
    if not dckey in quantile_cache:
        if dataset == 'yeargroups':
            g = yeargroups
        elif dataset == 'hourgroups':
            g = hourgroups
        else:
            raise ValueError ("Dataset '%s' unknown" % dataset)
        quantile_cache[dckey] = g.quantile(q)
    return quantile_cache[dckey]

for q, name in quantile_setup:
    fname = name.split()[0].lower()
    #do_plots (get_quantiles ('yeargroups', q), "%s by year: %%s" % name, "/tmp/year_length_%s-%%s.pdf" % fname)
    #do_plots (get_quantiles ('hourgroups', q), "%s by time of day: %%s" % name, "/tmp/tod_length_%s-%%s.pdf" % fname)
    do_heatmaps (get_quantiles ('yeargroups', q), "%s size and count by hour\n %%s" % name, "/tmp/hm_%s-%%s.pdf" % fname)
    with plt.xkcd():
        do_heatmaps (get_quantiles ('yeargroups', q), "%s size and count by hour\n %%s" % name, "/tmp/xhm_%s-%%s.pdf" % fname)
