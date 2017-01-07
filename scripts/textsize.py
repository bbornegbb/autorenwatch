data = spark.sql("""select year(date) as year, length(text) as len, type
                           from tagesschau
                           where 2000 < year(date) and year(date) < 2015""")\
            .toPandas()

types = sorted (set(data['type']))

for type_ in types:
    d = data.query('type == "%s"' % type_)
    years = sorted (set(d['year']))
    xpos = np.arange(len(years))+1
    ypos = max(d['len']) * 0.8
    cnts = [ len(d.query('year == %f' % year)) for year in years ]
    #
    fig, axes = plt.subplots (nrows=2, ncols=1, sharex=True, figsize=(5, 6),
                              gridspec_kw={'left': 0.17, 'right': 0.95, 'height_ratios': (2, 5), 'hspace': 0.1})
    axes[0].bar (xpos, cnts)
    axes[0].set_ylabel ("Count")
    axes[1].violinplot ([d.query('year == %f' % year)['len'] for year in years], showmedians=True)
    axes[1].set_ylabel ("Length")
    plt.xticks (xpos, years, rotation=30)
    axes[0].set_title ("Article type: " + type_)
    plt.savefig("/tmp/textsize-%s.pdf" % type_)
    plt.close()


