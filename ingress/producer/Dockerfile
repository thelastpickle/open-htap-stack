FROM python:3.12-slim

RUN apt-get -o Acquire::Retries=5 update && apt-get install -y wget unzip
RUN pip install --no-cache-dir --retries 5 kafka-python-ng==2.2.2 numpy==1.26.4 lz4==4.3.3 cassandra-driver==3.29.2

WORKDIR /app

# 322 MB from a single small host; retry rather than fail the build over it
RUN wget -qc --https-only --tries=5 --waitretry=15 --retry-connrefused --timeout=60 \
    https://mattmahoney.net/dc/enwik9.zip
RUN unzip enwik9.zip
RUN rm enwik9.zip

# A small curated corpus spanning several unrelated subjects.  It is the default
# because the dashboard's vector search only says anything if the snippets differ
# in meaning; enwik9 above is still there for volume testing (TEXT_FILE=/app/enwik9).
COPY wikipedia.txt /app/wikipedia.txt

#### CONFIGURATION #######################################
ENV KAFKA_BOOTSTRAP="kafka:19092"
ENV TOPIC="demo-events"
# throughput + cardinality.  N_ENTITIES is the starting fleet size and the
# dashboard may change it at runtime; MAX_ENTITIES is the ceiling it may not
# exceed, and fixes how much state this process allocates up front.
ENV EVENTS_PER_SEC=2000
ENV N_ENTITIES=100
ENV MAX_ENTITIES=2000
# percentage of readings carrying an anomalous internal temperature
ENV OUTLIER_PERCENT=5.0
# batching cadence (50ms batches for smooth motion)
ENV BATCH_PERIOD_MS=50
# optional kafka perf knobs (demo-friendly)
ENV KAFKA_ACKS=0
ENV KAFKA_LINGER_MS=20
ENV KAFKA_BATCH_SIZE=65536
ENV KAFKA_COMPRESSION=lz4
# text payload sampled per entity
ENV TEXT_FILE="/app/wikipedia.txt"
ENV TEXT_REFRESH_MIN_S=5
ENV TEXT_REFRESH_MAX_S=30
# Live demo controls, polled from the dashboard backend.
# Leave blank to ignore the dashboard and stay on the values above.
ENV SETTINGS_URL=""
ENV SETTINGS_POLL_INTERVAL_S=10
##########################################################

COPY producer.py /app/producer.py

CMD ["python", "-u", "/app/producer.py"]
