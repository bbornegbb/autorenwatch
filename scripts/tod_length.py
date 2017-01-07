types = filter (None, (row['type'] for row in tagesschau.select('type').distinct().collect()) )

groupby = "type year hour".split()

dates = spark.createDataFrame([ (t, y, h) for y in range(2000, 2016) for h in range (24) for t in types],
                              groupby)

data = spark.sql ("""select type, year(date) as year, hour(date) as hour, length(text) as len
                            from tagesschau
                            where 2000 < year(date) and year(date) < 2015""")\
            .join (dates, groupby, "right")\
            .toPandas()

colors = matplotlib.colors.cnames.keys()

def do_plot (d, title, t, outputpattern):
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
    plt.savefig (outputpattern % t)
    plt.close()

def do_plots(quantiles, titlepattern, outputpattern):
    for t in types:
        do_plot (quantiles.ix[t], titlepattern % t, t, outputpattern)

hourgroups = data.groupby (('type', 'year', 'hour'))
yeargroups = data.groupby (('type', 'hour', 'year'))

quantiles = ((0.25, "First quarter"),
             (0.5, "Median"),
             (0.75, "Third quarter")
)

for q, name in quantiles:
    fname = name.split()[0].lower()
    do_plots (yeargroups.quantile(q), "%s by year: %%s" % name, "/tmp/year_length_%s-%%s.pdf" % fname)
    do_plots (hourgroups.quantile(q), "%s by time of day: %%s" % name, "/tmp/tod_length_%s-%%s.pdf" % fname)
