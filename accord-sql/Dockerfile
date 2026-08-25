# GEICO's cassandra-sql: Postgres-dialect SQL, with serializable transactions,
# over Cassandra used as an ordered key-value store.  It speaks the Postgres wire
# protocol, so `psql` is its client.
#
# Built from a public repository pinned by revision, so a reader of this
# repository can fetch the same source.  Nothing here is vendored.
FROM docker.io/library/eclipse-temurin:17-jdk AS build

# git only; the Gradle wrapper fetches Gradle 8.4 and the dependencies itself.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG CASSANDRA_SQL_REPO=https://github.com/geico/cassandra-sql
ARG CASSANDRA_SQL_REV=a0257ec9a22ff84daaf6f529ae8b523fdc45b431
RUN git init . \
 && git remote add origin "${CASSANDRA_SQL_REPO}" \
 && git fetch --depth 1 origin "${CASSANDRA_SQL_REV}" \
 && git checkout FETCH_HEAD

# One patch, and the whole diff is in patches/ so a reader can see it without
# fetching anything.  cassandra-sql opens two Cassandra sessions: the one in
# CassandraConfig reads cassandra.contact-points, and the one in
# CassandraExecutor hard-coded localhost:9042.  The second is a @Component with
# an unconditional @PostConstruct, so in a container it fails the cassandraExecutor
# bean and the application never starts: "Cannot connect to Cassandra. Make sure
# it's running on localhost:9042".  The patch gives it the same four properties
# the first one already reads.  It is worth upstreaming and has not been yet.
COPY patches/ /patches/
RUN git apply --verbose /patches/*.patch

# -x test on purpose, and not to save time.  `*IntegrationTest` needs a Cassandra
# on localhost:9042, which a build stage has not got.  Of the unit tests, one is a
# statement about this stack that is deliberately false: CassandraConfigTest
# asserts system_views.settings reports ByteOrderedPartitioner, and this stack
# runs Murmur3Partitioner.  See the comment in podman-compose.yml for why that is
# safe; the short of it is that no code under src/main/ reads the partitioner.
RUN ./gradlew --no-daemon clean build -x test

FROM docker.io/library/eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/build/libs/cassandra-sql-mvp-0.1.0-SNAPSHOT.jar /app/cassandra-sql.jar

# 5432 is the Postgres wire listener, and it is not configurable: it is
# `private static final int POSTGRES_PORT = 5432` at
# src/main/java/com/geico/poc/cassandrasql/postgres/PostgresProtocolServer.java:23.
# The project's README implies a `cassandra-sql.protocol.port` setting; no such
# key is read.  8080 is Spring Boot's own HTTP port, which carries
# /api/sql/health and /api/sql/execute.
EXPOSE 5432 8080

# Xmx because the JVM's default is a quarter of the container limit, and the
# background jobs in application-kv.yml scan whole tables.
#
# The driver's request timeout is raised from its 2-second default because that
# default is what stopped this service on a cold stack.  `CassandraExecutor.init`
# opens its own session and issues `CREATE KEYSPACE IF NOT EXISTS` inside one
# request budget; against a Cassandra that had only just opened its port the
# request answered too late, and the bean failed the application with "Cannot
# connect to Cassandra. Make sure it's running on cassandra:9042", caused by
# "DriverTimeoutException: Query timed out after PT2S" at init:65.
# `depends_on: service_healthy` does not prevent it: a node that answers cqlsh is
# not thereby a node that answers a schema change in two seconds.
#
# That the property reaches this application's sessions was measured rather than
# assumed.  A container run with `=1ms` printed "timed out after PT0.001S" from
# both of them, `s0` in CassandraConfig and `s1` in CassandraExecutor, so the
# driver's default config loader does read a system property here.  The price is
# that a statement which would have failed at 2 s now fails at 30 s.
#
# No value may contain a space: the entrypoint expands JAVA_OPTS unquoted, so
# "30 seconds" would split into two arguments.  HOCON accepts the bare unit.
ENV JAVA_OPTS="-Xmx1g -XX:+UseG1GC -Ddatastax-java-driver.basic.request.timeout=30s"

# The entrypoint is a supervisor rather than an `exec java`, because this
# application can fail to start without exiting; the script says why.
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
CMD ["/app/entrypoint.sh"]
