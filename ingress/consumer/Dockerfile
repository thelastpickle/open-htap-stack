FROM python:3.14-slim

RUN pip install --no-cache-dir --retries 5 kafka-python-ng==2.2.3 cassandra-driver==3.30.1 lz4==4.4.5

WORKDIR /app
COPY consumer.py /app/consumer.py

CMD ["python", "-u", "/app/consumer.py"]
