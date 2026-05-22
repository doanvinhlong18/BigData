FROM apache/spark:3.5.1

USER root

# Ensure a complete Python runtime (stdlib + encodings) is available
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Explicit PySpark Python to avoid ambiguous interpreter selection
ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

# ── Delta Lake Python shim (local, tránh pip trên base image Spark) ─────────
ENV PYTHONPATH=/opt/spark/app
COPY delta /opt/spark/app/delta

# ── JAR dir ────────────────────────────────────────────
ENV JARS=/opt/spark/jars
RUN mkdir -p ${JARS}

# Delta
RUN curl -fsSL -o ${JARS}/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar

RUN curl -fsSL -o ${JARS}/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# Kafka
RUN curl -fsSL -o ${JARS}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

RUN curl -fsSL -o ${JARS}/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

RUN curl -fsSL -o ${JARS}/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN curl -fsSL -o ${JARS}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# MinIO / S3A
RUN curl -fsSL -o ${JARS}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -fsSL -o ${JARS}/aws-java-sdk-bundle-1.12.517.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar

COPY conf/spark-defaults.conf /opt/spark/conf/

ENV HADOOP_USER_NAME=root

RUN mkdir -p /opt/spark/app/spark/jobs