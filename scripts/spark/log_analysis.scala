import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.types._

val raw = spark.read.text("hdfs://namenode:9000/data/logs/web_server_logs.txt")
raw.show(5, truncate = false)
raw.count()

val sampleLines = raw.take(5).map(_.getString(0))
sampleLines.foreach(println)

val logPattern = """^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)$"""

val testLine = sampleLines(0)
val m = logPattern.r.findFirstMatchIn(testLine)
m.foreach(g => println(g.subgroups))

val parsed = raw.select(
  regexp_extract($"value", logPattern, 1).as("ip"),
  regexp_extract($"value", logPattern, 2).as("timestamp_raw"),
  regexp_extract($"value", logPattern, 3).as("method"),
  regexp_extract($"value", logPattern, 4).as("url"),
  regexp_extract($"value", logPattern, 5).as("protocol"),
  regexp_extract($"value", logPattern, 6).as("status").cast("int"),
  regexp_extract($"value", logPattern, 7).as("response_size")
)

parsed.show(5, truncate = false)

val parquetPath = "hdfs://namenode:9000/data/logs/parsed_parquet"
Try {
  parsed.repartition(4).write.mode("overwrite").parquet(parquetPath)
} match {
  case Success(_) => println(s"Wrote parquet to $parquetPath")
  case Failure(e) =>
    println(s"Warning: failed to write to $parquetPath: ${e.getMessage}")
    e.printStackTrace()
}

val tableName = "default.parsed_logs_v2"

val customSchema = StructType(Array(
  StructField("ip", StringType, true),
  StructField("timestamp_raw", StringType, true),
  StructField("method", StringType, true),
  StructField("url", StringType, true),
  StructField("protocol", StringType, true),
  StructField("status", IntegerType, true),
  StructField("response_size", StringType, true)
))

if (spark.catalog.tableExists(tableName)) {
  spark.catalog.dropTempView(tableName)
  spark.sql(s"DROP TABLE IF EXISTS $tableName")
}

spark.catalog.createTable(
  tableName = tableName,
  source = "hive",                        // <--- Tells Spark to write a real Hive Metastore entry
  schema = customSchema,
  options = Map(
    "path" -> parquetPath,                // <--- Dictates EXTERNAL table type status
    "fileFormat" -> "parquet"             // <--- Informs Hive/Trino how to read file contents
  )
)

spark.catalog.refreshTable(tableName)

spark.table(tableName).show(5)
