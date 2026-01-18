FROM python:3.12-slim

RUN pip install --no-cache-dir kafka-python-ng==2.2.2 cassandra-driver==3.29.2

WORKDIR /app
COPY consumer.py /app/consumer.py

CMD ["python", "-u", "/app/consumer.py"]
