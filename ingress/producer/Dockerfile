FROM python:3.14-slim

RUN apt-get -o Acquire::Retries=5 update && apt-get install -y wget unzip
RUN pip install --no-cache-dir --retries 5 kafka-python-ng==2.2.3 numpy==2.5.2 lz4==4.4.5 cassandra-driver==3.30.1

WORKDIR /app

# A small curated corpus spanning several unrelated subjects.  It is the default
# because the dashboard's vector search only says anything if the snippets differ
# in meaning.
COPY wikipedia.txt /app/wikipedia.txt

# enwik9 is 322 MB from one small host, and it is here only for volume testing
# (TEXT_FILE=/app/enwik9), so the fetch is off by default:
#   podman compose build --build-arg FETCH_ENWIK9=1 data-producer
#
# It was unconditional, and it failed CI.  wget wrote a file in 150 ms and exited 0,
# where the same step had taken 13.7 s and succeeded six hours earlier, so the host
# answered that runner with something short and the build died one step later at
# "unzip: cannot find zipfile directory in one of enwik9.zip".  -q had discarded
# whatever the server said, and no checksum caught the difference.  Three build
# attempts could not help either, because the bad download sat in a layer the second
# and third reused.
#
# So: one RUN, so a bad download leaves no layer behind; a checksum, so a short body
# fails where it happens rather than at the unzip; and -c dropped, since continuing a
# file that may be a block page is how the two get concatenated.  The archive has not
# changed since 2023-02-24 and is 322,592,222 bytes.
ARG FETCH_ENWIK9=0
ARG ENWIK9_SHA256=99cdb5ac84392252d3f0912ccedd195bc95bd80cbef3b0cdf2eee4ad9a3b7a51
RUN if [ "${FETCH_ENWIK9}" = "1" ]; then \
      wget -q --show-progress --https-only --tries=5 --waitretry=15 \
           --retry-connrefused --timeout=60 https://mattmahoney.net/dc/enwik9.zip \
      && echo "${ENWIK9_SHA256}  enwik9.zip" | sha256sum -c - \
      && unzip -q enwik9.zip \
      && rm enwik9.zip; \
    fi

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
