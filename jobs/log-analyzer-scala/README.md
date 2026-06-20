# Log Analyzer Scala Spark Job

This job sessionizes web server logs (CLF format) by IP, computes session durations, and writes results to Hive and Parquet.

## Usage

Build the fat JAR:

```sh
cd jobs/log-analyzer-scala
sbt assembly
```

### Arguments
- `--input` (default: `/data/logs/web_server_logs.txt`): Input log file (HDFS path)
- `--hiveDb` (default: `default`): Hive database for output tables
- `--sessionGapMinutes` (default: 30): Session gap in minutes
- `--outputFormat` (default: `parquet`): Output format for Parquet files
- `--overwrite`: Overwrite output tables/files

### Run with Docker

```sh
docker build --platform linux/amd64 -t log-analyzer-scala .
```

```sh
docker run --rm \
  --network docker-hadoop-spark_default \
  -e ENABLE_INIT_DAEMON=false \
  -e SPARK_APPLICATION_ARGS="--input hdfs://namenode:9000/data/logs/web_server_logs.txt --hiveDb default --sessionGapMinutes 30" \
  log-analyzer-scala
```

## Output
- Hive tables: `<hiveDb>.sessions` (partitioned by job_run_date), `<hiveDb>.session_summary` (partitioned by job_run_date)
- Parquet: `/data/output/sessionization/` (partitioned by job_run_date)

Note: Each run adds job_run_id (UUID) and job_run_date columns; tables are partitioned by job_run_date.
