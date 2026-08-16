# copy (see below) sidecar from official image
FROM ghcr.io/apache/cassandra-sidecar@sha256:24a080b9333ddc16822f3e1ab1ff53bef31db023c4a5475c49c7f84efbbf3e42 AS sidecar
# base image
FROM eclipse-temurin:17-jdk

ENV CASSANDRA_NIGHTLY_TGZ_URL="https://archive.apache.org/dist/cassandra/5.0.8/apache-cassandra-5.0.8-bin.tar.gz"
ENV CASSANDRA_HOME=/opt/cassandra
ENV PATH="${CASSANDRA_HOME}/bin:${PATH}"

# retries: these reach the Ubuntu archive and Launchpad, and the scheduled CI build
# runs without a layer cache, so a transient fetch failure fails the whole build
RUN apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates python3.11 procps netcat-openbsd sudo \
    && rm -rf /var/lib/apt/lists/*

# copy sidecar application (jib files hardcode this "/app" path)
COPY --from=sidecar /app /app

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --set python3 /usr/bin/python3.11

RUN mkdir -p /opt \
    && echo "Downloading: ${CASSANDRA_NIGHTLY_TGZ_URL}" \
    && curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors "${CASSANDRA_NIGHTLY_TGZ_URL}" -o /tmp/cassandra.tgz \
    && tar -xzf /tmp/cassandra.tgz -C /opt \
    && rm /tmp/cassandra.tgz \
    && mv /opt/apache-cassandra-* "${CASSANDRA_HOME}"

RUN useradd -m -u 999 cassandra \
    && chown -R cassandra:cassandra "${CASSANDRA_HOME}" \
    && echo "cassandra ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER cassandra
EXPOSE 7000 9042 9043
ENTRYPOINT ["/entrypoint.sh"]
