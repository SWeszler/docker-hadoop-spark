# Infrastructure Modernization Brief

## Current State & Challenges
The current local infrastructure relies on the `bde2020` (Big Data Europe) Docker images. While excellent for local training and learning, this ecosystem is showing its age. 

**Key Issues with Current Stack:**
1. **Outdated Software:** Relying on Hadoop 3.2.1, Spark 3.0.0, and Java 8. These versions lack modern features, performance improvements, and syntax enhancements.
2. **Security Vulnerabilities:** Because the `bde2020` project is no longer actively maintained, the images contain unpatched CVEs (such as Log4Shell) and run on outdated Linux base images.
3. **Orchestration:** Docker Compose is great for local prototyping, but lacks the self-healing, scaling, and resource management capabilities required for a modern production big data cluster.

## Modernization Goals
The goal is to transition the cluster to a modern, secure, and actively maintained ecosystem while retaining the ease of local deployment.

### Phase 1: Image & Version Upgrades (Local Docker)
* **Replace `bde2020` Images:** Transition away from the abandoned `bde2020` repository. 
  * **Spark:** Move to the actively maintained **Bitnami Apache Spark images** (`bitnami/spark:3.5.x`). Bitnami provides secure, constantly updated, and production-ready containers.
  * **Hadoop:** Move to the official Apache Hadoop images or build custom, minimal Dockerfiles based on the latest Hadoop releases (e.g., `3.3.x`).
* **Runtime Upgrade:** Upgrade the underlying Java runtime from Java 8 to **Java 11 or Java 17**, ensuring better garbage collection and performance.
* **Component Upgrades:**
  * Upgrade Spark to `3.4.x` or `3.5.x`.
  * Upgrade Presto to **Trino** (the modern, more active fork of Presto).

### Phase 2: Orchestration (Kubernetes)
* **Move to Kubernetes (K8s):** Modern big data stacks run heavily on Kubernetes rather than YARN. 
* **Helm Charts:** Utilize official Helm charts (like the Bitnami Spark Helm chart) to deploy the Spark and Hadoop ecosystem on a local Kubernetes cluster (like `minikube` or Docker Desktop's built-in K8s) to simulate a true modern production environment.
* **Spark on K8s:** Transition from Spark Standalone / YARN to Spark's native Kubernetes scheduler.

### Phase 3: Storage & Format Modernization
* **Cloud Storage Simulation:** Introduce a local S3-compatible object storage layer (like **MinIO**) to replace or complement HDFS. Modern architectures favor decoupled compute (Spark) and storage (S3) over tightly coupled HDFS.
* **Table Formats:** Introduce modern open table formats like **Apache Iceberg** or **Delta Lake** into the Spark and Trino processing pipelines, replacing raw Parquet/Hive tables.

### Phase 4: Custom Data Platform UI (Orchestration & Management)
* **Unified Control Plane:** Build a modern, highly interactive web application to serve as the "Control Center" for the data platform.
* **Pipeline Management (DAGs):** Provide a visual node-and-wire interface (using libraries like `React Flow`) to construct, trigger, and monitor DAG-based data pipelines.
* **Orchestration Integration (Apache Airflow):** Instead of building a custom DAG engine from scratch, the custom UI will interact directly with a headless **Apache Airflow** instance via its REST API. 
  * *Why do we need Airflow?* Current infrastructure tools (like Hadoop, Spark, and Hive) are strictly *execution engines*—they process data quickly but have no concept of time, schedules, retries, or cross-job dependencies. Airflow acts as the "Traffic Controller." It sits above the compute layer to ensure that Job B only runs if Job A succeeds, manages cron schedules, and handles failure alerts. Airflow handles this orchestration heavy lifting, while the custom UI provides a simplified, premium user experience.
* **Tech Stack:** Build the frontend using a modern framework (like **React/Next.js** or **Vite**) with premium aesthetics, dynamic micro-animations, and a sleek dark mode.

## Next Steps
1. Create a branch to test swapping the `bde2020/spark-master` and `spark-worker` with `bitnami/spark` in the `docker-compose.yml`.
2. Validate that the current Scala/Python job templates still successfully submit to the new Spark version.
3. Scaffold the foundational web project for the Data Platform UI.
