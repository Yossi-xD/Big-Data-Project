FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orders_producer.py .
COPY data ./data

CMD ["python", "orders_producer.py"]
