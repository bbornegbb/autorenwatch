def reattach ():
    global tagesschau
    global tagesschau_short
    global authors
    global json_files
    tagesschau = spark.read.parquet ("db/tagesschau")
    tagesschau.registerTempTable("tagesschau")
    tagesschau_short = spark.read.parquet ("db/tagesschau_short")
    tagesschau_short.registerTempTable("tagesschau_short")
    authors = spark.read.parquet ("db/authors")
    authors.registerTempTable("authors")
    json_files = spark.read.parquet("db/json_files")
    json_files.registerTempTable("json_files")
    json_files20k = json_files.repartition(20000)
    json_files20k.registerTempTable ("json_files20k")

reattach()
