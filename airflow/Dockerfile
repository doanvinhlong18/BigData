# airflow/Dockerfile
FROM apache/airflow:2.8.0-python3.11

USER root
# Thư viện system cần cho psycopg2, lightgbm
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Bake toàn bộ pip dependencies vào image
# → không còn pip install mỗi lần restart
RUN pip install --no-cache-dir \
    deltalake==0.17.4 \
    lightgbm==4.3.0 \
    scikit-learn==1.4.2 \
    pandas==2.2.1 \
    pyarrow==15.0.0 \
    requests==2.31.0 \
    mlflow==2.12.1 \
    holidays==0.46 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9 \
    numpy==1.26.4