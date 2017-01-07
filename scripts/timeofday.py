from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

timeofday = spark.sql("select year(date) as year, hour(date) as hour from tagesschau").groupBy ("year", "hour").count().orderBy("year", "hour").toPandas()
tdquery = timeofday.query('2000 < year < 2015')

colors = np.array (list('bgrcmykw'*4))
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.bar (tdquery['hour'], tdquery['count'], tdquery['year'],
        zdir='y', color=colors[tdquery['year']-2000], alpha=0.7)
ax.set_xlabel ("Time of day (hour)")
ax.set_ylabel ("Year")
ax.set_zlabel ("Number of articles")
plt.savefig("/tmp/timeofday.pdf")
plt.close()

