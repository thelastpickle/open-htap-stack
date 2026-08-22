# base image: Cassandra 5.0 supports Java 11 and 17 only, so temurin 17 stays
FROM eclipse-temurin:17-jdk

# Both tarballs come from archive.apache.org, not dlcdn: dlcdn drops a release as soon as
# the next one lands, and a scheduled CI build must still fetch the pinned version months on.
ENV CASSANDRA_VERSION=5.0.9
ENV CASSANDRA_TGZ_URL="https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
ENV CASSANDRA_TGZ_SHA256=eee1460b47ebe188a29521207230617f22aad7f9e1674f5ea454f10c8f344d61
ENV CASSANDRA_HOME=/opt/cassandra

# Sidecar was a digest of the ghcr `latest` tag, which that repository never version-tags,
# so the image could not be named or reproduced.  0.4.0 is a release with checksums beside it.
ENV SIDECAR_VERSION=0.4.0
ENV SIDECAR_TGZ_URL="https://archive.apache.org/dist/cassandra/cassandra-sidecar/${SIDECAR_VERSION}/apache-cassandra-sidecar-${SIDECAR_VERSION}.tar.gz"
ENV SIDECAR_TGZ_SHA256=feeeebecc21becd38c20ba6095030691654e2fe94e3fc8f13181d5a71c99a6bd
ENV SIDECAR_HOME=/opt/cassandra-sidecar

ENV PATH="${CASSANDRA_HOME}/bin:${SIDECAR_HOME}/bin:${PATH}"

# retries: these reach the Ubuntu archive and Launchpad, and the scheduled CI build
# runs without a layer cache, so a transient fetch failure fails the whole build
RUN apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates python3.11 procps netcat-openbsd sudo \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --set python3 /usr/bin/python3.11

RUN mkdir -p /opt \
    && echo "Downloading: ${CASSANDRA_TGZ_URL}" \
    && curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors "${CASSANDRA_TGZ_URL}" -o /tmp/cassandra.tgz \
    && echo "${CASSANDRA_TGZ_SHA256}  /tmp/cassandra.tgz" | sha256sum -c - \
    && tar -xzf /tmp/cassandra.tgz -C /opt \
    && rm /tmp/cassandra.tgz \
    && mv "/opt/apache-cassandra-${CASSANDRA_VERSION}" "${CASSANDRA_HOME}"

RUN echo "Downloading: ${SIDECAR_TGZ_URL}" \
    && curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors "${SIDECAR_TGZ_URL}" -o /tmp/sidecar.tgz \
    && echo "${SIDECAR_TGZ_SHA256}  /tmp/sidecar.tgz" | sha256sum -c - \
    && tar -xzf /tmp/sidecar.tgz -C /opt \
    && rm /tmp/sidecar.tgz \
    && mv "/opt/apache-cassandra-sidecar-${SIDECAR_VERSION}" "${SIDECAR_HOME}"

RUN useradd -m -u 999 cassandra \
    && chown -R cassandra:cassandra "${CASSANDRA_HOME}" "${SIDECAR_HOME}" \
    && echo "cassandra ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER cassandra
EXPOSE 7000 9042 9043
ENTRYPOINT ["/entrypoint.sh"]
