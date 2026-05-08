FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY unified_producer.py .

CMD ["python", "unified_producer.py", "request"]