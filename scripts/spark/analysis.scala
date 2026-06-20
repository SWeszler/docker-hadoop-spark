val df = spark.read
    .option("header", "true")
		.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")

df.show()
df.select("_c1", "_c2").show()

val dfWithYear = df.withColumn("started_year", (rand() * 100 + 1900).cast("int"))

dfWithYear.repartition(4).write.mode("overwrite").parquet("hdfs://namenode:9000/data/openbeer/breweries_with_year")
spark.read.parquet("hdfs://namenode:9000/data/openbeer/breweries_with_year").show()