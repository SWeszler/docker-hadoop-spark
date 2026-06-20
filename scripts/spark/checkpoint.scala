// Configure checkpoint directory
spark.sparkContext.setCheckpointDir("hdfs://namenode:9000/checkpoint")

// Create a sample DataFrame with some transformations
val df = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")

// Cache the DataFrame
df.cache()
// or alternatively use persist
// df.persist(StorageLevel.MEMORY_AND_DISK)

// Create a complex transformation chain
val processedDF = df
  .select("_c1", "_c2")
  .filter($"_c1".isNotNull)
  .groupBy("_c2")
  .count()
  .withColumn("timestamp", current_timestamp())

// Mark this DataFrame for checkpointing
processedDF.checkpoint()

// Actions to demonstrate caching and checkpointing
println("First count (will cache data):")
println(processedDF.count())

println("\nSecond count (will use cached data):")
println(processedDF.count())

// Force unpersist to clear cache
df.unpersist()
df.explain(true)

// Show the checkpointed data
println("\nReading from checkpoint:")
processedDF.show()