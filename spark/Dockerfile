FROM bitnami/spark:3.5.0

USER root

# Delta Lake (Python API)
RUN pip install delta-spark==3.1.0

# Hadoop S3A + AWS SDK (required for MinIO)
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.517.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/

USER 1001
