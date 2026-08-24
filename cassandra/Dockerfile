# base image: Cassandra 6.0 ships jvm11-, jvm17- and jvm21-server.options, so temurin 17 stays
# a supported runtime and the Sidecar's own --add-opens set is still the Java 17 one.
FROM eclipse-temurin:17-jdk

# Cassandra comes from archive.apache.org, not dlcdn: dlcdn drops a release as soon as
# the next one lands, and a scheduled CI build must still fetch the pinned version months on.
ENV CASSANDRA_VERSION=6.0-alpha2
ENV CASSANDRA_TGZ_URL="https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
ENV CASSANDRA_TGZ_SHA256=cc83cf03aee056646f76bd9d346e616fd3c2543f4a74e84b1431290b934c02d1
ENV CASSANDRA_HOME=/opt/cassandra

# The Sidecar is built here and committed, because 6.0 support is not in any release.
# 0.4.0 has no 6.0 adapter, and the Sidecar refuses a node whose version no
# @MinimumVersion factory claims; upstream declares 0.5-SNAPSHOT, so this local build is
# 0.5-mck0.  dist/VENDOR.md records the fork, the commit and the build command.
ENV SIDECAR_VERSION=0.5-mck0
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

# The checksum is verified even though the file is copied rather than fetched: it is stored
# in Git LFS, and a checkout whose LFS objects did not arrive leaves a 130-byte pointer file
# in its place.  Checking here fails the build, instead of leaving a container that starts
# and then cannot find its launcher.
COPY dist/apache-cassandra-sidecar-${SIDECAR_VERSION}.tar.gz /tmp/sidecar.tgz
COPY dist/apache-cassandra-sidecar-${SIDECAR_VERSION}.tar.gz.sha256 /tmp/sidecar.tgz.sha256
RUN awk '{print $1"  /tmp/sidecar.tgz"}' /tmp/sidecar.tgz.sha256 | sha256sum -c - \
    && tar -xzf /tmp/sidecar.tgz -C /opt \
    && rm /tmp/sidecar.tgz /tmp/sidecar.tgz.sha256 \
    && mv "/opt/apache-cassandra-sidecar-${SIDECAR_VERSION}" "${SIDECAR_HOME}"

RUN useradd -m -u 999 cassandra \
    && chown -R cassandra:cassandra "${CASSANDRA_HOME}" "${SIDECAR_HOME}" \
    && echo "cassandra ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER cassandra
EXPOSE 7000 9042 9043
ENTRYPOINT ["/entrypoint.sh"]
