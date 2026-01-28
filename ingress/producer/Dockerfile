FROM python:3.12-slim

RUN apt-get update && apt-get install -y wget unzip
RUN pip install --no-cache-dir kafka-python-ng==2.2.2 numpy==1.26.4 lz4==4.3.3 cassandra-driver==3.29.2

WORKDIR /app

RUN wget -qc http://mattmahoney.net/dc/enwik9.zip
RUN unzip enwik9.zip
RUN rm enwik9.zip

#### CONFIGURATION #######################################
ENV KAFKA_BOOTSTRAP="kafka:19092"
ENV TOPIC="demo-events"
# throughput + cardinality
ENV EVENTS_PER_SEC=4000
ENV N_ENTITIES=2000
# batching cadence (2s batches by default)
ENV BATCH_PERIOD_MS=2000
# optional kafka perf knobs (demo-friendly)
ENV KAFKA_ACKS=0
ENV KAFKA_LINGER_MS=20
ENV KAFKA_BATCH_SIZE=65536
ENV KAFKA_COMPRESSION=lz4
# optional big text payload
ENV TEXT_FILE="/app/enwik9"
ENV TEXT_REFRESH_MIN_S=5
ENV TEXT_REFRESH_MAX_S=30
##########################################################

COPY producer.py /app/producer.py

CMD ["python", "-u", "/app/producer.py"]
