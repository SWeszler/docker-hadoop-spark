# Apache Spark & Hadoop Learning Challenge: Deep Dive into Web Server Log Analysis 🕵️‍♀️🌐

Your Mission: You are a data engineer at a rapidly growing online news portal. To better understand user engagement, identify popular content, and detect potential issues, you've been tasked with analyzing the web server access logs. Your goal is to build a robust Apache Spark application that can efficiently process these logs, extract meaningful insights, and be optimized for performance on your Hadoop cluster.

## Core Learning Focus
* **Spark Application Anatomy:** Understanding the roles of the Driver, Executors, SparkContext/SparkSession, Jobs, Stages, and Tasks. You'll be using the Spark UI extensively.
* **Parallelism in Spark:** Observing how Spark distributes data and computation, how partitioning works, and how you can influence it.
* **Spark Job Optimization:** Implementing various techniques to make your Spark jobs run faster and more efficiently, such as choosing appropriate data formats, caching, managing shuffles, and using efficient transformations.

## Challenge Description 📝
You will process web server access logs, typically in a Common Log Format (CLF). Your Spark application should perform the following analyses:

### Traffic Summary
* Count the total number of requests.
* Count the number of unique IP addresses (visitors).
* Calculate the total amount of data transferred (sum of response sizes).

### Content Popularity
* Identify the top 20 most requested URLs (paths).
* Identify the top 10 most popular resource types (e.g., based on file extensions like .html, .jpg, .css, .js).

### HTTP Status Code Analysis
* Count the occurrences of each HTTP status code (e.g., 200, 404, 500).
* Identify the top 10 IP addresses that generated the most 404 errors.

### Time-based Analysis
* Calculate the number of requests per hour of the day (0-23).
* Identify the busiest hour(s).

### (Optional Advanced) Sessionization
Attempt to group requests from the same IP address into sessions. Calculate the average session duration.

## The Data: Web Server Logs 📜
A common log format looks like this:
`IP_ADDRESS - USER_ID [TIMESTAMP] "REQUEST_METHOD URL PROTOCOL" STATUS_CODE RESPONSE_SIZE`

### Generating a Larger Dataset
To truly test Spark's capabilities, you'll need a larger dataset (e.g., millions of lines). A script is provided in this folder (`LogGenerator.scala`) to generate it for you.

Run the Scala script from your terminal:
```bash
scala challenge/LogGenerator.scala
```
This will create a large file named `web_server_logs.txt`.

### Getting Data into HDFS
1. Copy the generated file to your Docker namenode container:
   ```bash
   docker cp web_server_logs.txt namenode:/tmp/web_server_logs.txt
   ```
2. Access the namenode container's shell:
   ```bash
   docker exec -it namenode bash
   ```
3. Inside the namenode container, put the file into HDFS:
   ```bash
   hdfs dfs -mkdir -p /data/logs
   hdfs dfs -put /tmp/web_server_logs.txt /data/logs/web_server_logs.txt
   ```

## Hints for Solving & Learning Points 💡

### Parsing Logs
* The log format is fairly regular. You can use regular expressions (`regexp_extract` in Spark SQL or DataFrame API) to parse each line into its components.
* Define a schema for your parsed log data.
* Handle malformed lines gracefully.

### SparkSession and Initial Read
* Start by creating a `SparkSession`.
* Read the text file from HDFS using `spark.read.text("/data/logs/web_server_logs.txt")`. 

### DataFrame Transformations
* Use `withColumn` and functions from `pyspark.sql.functions` extensively.
* For extracting the resource type, parse the URL path.

### Understanding Spark UI (Key to this Challenge!)
Access it via `http://localhost:4040` or through the YARN ResourceManager UI (`http://localhost:8088`).
* **Jobs:** Each action triggers a job.
* **Stages:** Jobs are broken into stages at a shuffle boundary.
* **Tasks:** Each stage consists of parallel tasks.

### Parallelism Exploration
* **Default Partitions:** How many partitions does Spark create by default?
* **`repartition()` vs. `coalesce()`:** Experiment with `df.repartition(N)` or `df.coalesce(N)`.
* **`spark.sql.shuffle.partitions`:** Try changing it and observe the impact.

### Optimization Techniques - Iterative Improvement
* **Data Format (CSV/Text vs. Parquet):** Save your parsed DataFrame to Parquet format in HDFS and compare performance.
* **Caching (`.cache()` or `.persist()`):** Cache DataFrames used multiple times.
* **Shuffle Analysis:** Identify and reduce operations causing shuffles.
* **Efficient Functions:** Use built-in Spark SQL functions whenever possible. Avoid UDFs in Python if a native Spark function exists.

### Handling Timestamps
* Use `to_timestamp` with the correct format string (e.g., `'dd/MMM/yyyy:HH:mm:ss Z'`).

## Submitting Your Application (Productionizing)

Once your code is ready, you shouldn't just run it interactively forever. You should package it and submit it to the cluster! Here is the recommended step-by-step approach using the provided templates:

1. **Pick a Template:** Copy one of the language folders from `template/` (e.g., `template/python`) into a new directory for your job, like `dags/log-analyzer/`.
2. **Add Your Code:** Place your finalized Spark script (e.g., `main.py` or your compiled `.jar`) into this new directory.
3. **Update the Dockerfile:** Edit the `Dockerfile` inside your new directory to copy your script/jar into the image, and ensure the environment variables (like `SPARK_APPLICATION_PYTHON_LOCATION` or `SPARK_APPLICATION_MAIN_CLASS`) point to your code.
4. **Build the Job Image:**
   ```bash
   cd dags/log-analyzer/
   docker build -t my-log-analyzer .
   ```
5. **Submit the Job:** Run your new image, connecting it to the same Docker network as your Spark cluster:
   ```bash
   docker run --network docker-hadoop-spark_default \
              -e SPARK_MASTER_NAME=spark-master \
              -e SPARK_MASTER_PORT=7077 \
              my-log-analyzer
   ```

This container will automatically run `spark-submit` against your `spark-master` node, execute the job, and then gracefully exit. You can monitor the job's progress on the Spark UI (`http://localhost:8080`).

---

This challenge is designed to be iterative. Start with the basics, get it working, then progressively explore the parallelism and optimization aspects using the Spark UI as your guide. Good luck, and have fun diving into the world of Spark!
