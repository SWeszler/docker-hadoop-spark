# --- STAGE 1: Build ---
# Fixed: Using a valid, exact tag for Java 11, sbt, and Scala 2.12
FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.17_8_1.8.2_2.12.17 AS builder

WORKDIR /app
COPY . /app

# Build the fat JAR
RUN sbt clean assembly

# --- STAGE 2: Runtime ---
FROM bde2020/spark-submit:3.0.0-hadoop3.2

WORKDIR /app

# Copy assembled jar from builder
COPY --from=builder /app/target/scala-2.12/*-assembly-*.jar /app/target/

# Copy submit helpers
COPY run.sh submit-client.sh submit-cluster.sh /
RUN chmod +x /run.sh /submit-client.sh /submit-cluster.sh
COPY hive-site.xml /spark/conf/hive-site.xml
COPY spark-defaults.conf /spark/conf/spark-defaults.conf

ENV SPARK_APPLICATION_MAIN_CLASS=com.example.SessionAnalysis

CMD ["/run.sh"]
