
FROM apache/spark:3.5.1

USER root

# ── Python binding cho Delta Lake ──────────────────────────
# bitnami/spark đã có sẵn PySpark, chỉ cần cài delta-spark Python API
RUN pip install --no-cache-dir delta-spark==3.1.0

# ── Thư mục JARs của Spark ─────────────────────────────────
ENV JARS=/opt/spark/jars

# Tạo thư mục nếu chưa tồn tại
RUN mkdir -p ${JARS}

# [1] Delta Lake core (Scala 2.12, Spark 3.5)
RUN curl -fsSL -o ${JARS}/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar

# [2] Delta storage (luôn đi kèm delta-spark)
RUN curl -fsSL -o ${JARS}/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# [3] Spark Kafka connector – THIẾU FILE NÀY SẼ LỖI "format kafka not found"
RUN curl -fsSL -o ${JARS}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# [4] Kafka client (dep bắt buộc của [3])
RUN curl -fsSL -o ${JARS}/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# [5] Commons pool (dep bắt buộc của [3])
RUN curl -fsSL -o ${JARS}/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# [6] Token provider streaming Kafka (dep bắt buộc của [3])
RUN curl -fsSL -o ${JARS}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# [7] Hadoop AWS S3A – kết nối MinIO
RUN curl -fsSL -o ${JARS}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# [8] AWS Java SDK (dep bắt buộc của [7])
RUN curl -fsSL -o ${JARS}/aws-java-sdk-bundle-1.12.517.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar

# ── Copy spark config ───────────────────────────────────────
COPY conf/spark-defaults.conf /opt/spark/conf/

# ── Run as root (simplest solution) ──────────────────────────
# No need to switch user - run everything as root
# This avoids Hadoop UserGroupInformation issues
