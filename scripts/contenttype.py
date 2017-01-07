import pyspark.sql.functions as F
import matplotlib.pyplot as plt

dates = spark.createDataFrame([ (y, m) for y in range(2000, 2016) for m in range (1, 13) ], "year month".split())
dates = dates.crossJoin(tagesschau.select ("type").where(~ F.isnull(tagesschau['type']) ).distinct())

data = spark.sql("""select count(*) as count, type, year(date) as year, month(date) as month
                           from tagesschau
                           group by year(date), month(date), type""")\
            .join (dates, "year month type".split(), "right")\
            .orderBy ("year", "month", "type")\
            .toPandas()
data['time'] = data['year'] + (data['month']-1)/12.
data.fillna (0, inplace=True)

types1 = "image video story audio broadcast".split()
types2 = "link box imagegallery poll correspondent".split()

def make_plots(data, types, output=None):
    for t in types:
        d = data.query("type == '%s'" % t)
        plt.plot (d['time'], d['count'], label = t)
    plt.xlim (2000,2015)
    plt.legend (loc="upper left", shadow=True)
    plt.xlabel ("Time (years)")
    plt.ylabel ("Count")
    if output:
        plt.savefig(output)
        plt.close()
    else:
        plt.show()

for types in (types1, types2):
    make_plots (data, types)

make_plots (data, types1, "/tmp/content_type1.pdf")
make_plots (data, types2, "/tmp/content_type2.pdf")

