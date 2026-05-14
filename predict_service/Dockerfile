# predict_service/Dockerfile
# Dùng cùng Python version với airflow để tránh dependency conflict
FROM python:3.11-slim

WORKDIR /app

# System deps cho psycopg2, lightgbm
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Giữ đúng phiên bản với airflow/Dockerfile để tránh conflict
RUN pip install --no-cache-dir \
    deltalake==0.17.4 \
    lightgbm==4.3.0 \
    scikit-learn==1.4.2 \
    pandas==2.2.1 \
    pyarrow==15.0.0 \
    mlflow==2.12.1 \
    holidays==0.46 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9 \
    numpy==1.26.4

COPY predict_service.py .

CMD ["python", "-u", "predict_service.py"]