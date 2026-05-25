FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==3.12.0 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9

CMD ["mlflow", "server"]
