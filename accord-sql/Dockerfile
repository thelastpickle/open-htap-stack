# A placeholder for Accord SQL, the Postgres-dialect SQL prototype over Accord
# transactions.  Nothing in podman-compose.yml references this image, so nothing
# starts it; the directory holds the decisions taken about it, not a build.
#
# It is deferred, and the reason is no longer Cassandra's version.  The stack ran
# 5.0 for one: 6.0 writes BTI SSTables at version "ea", which cqlite could not
# parse and for which cassandra-analytics had no bridge, so moving to 6.0 took
# the cqlite and spark_bulk paths with it.  Both now read ea, so the stack runs
# 6.0-alpha2, and lib/cassandra-accord-6.0-alpha2.jar ships with it: cqlsh parses
# BEGIN TRANSACTION and the node refuses it with "Accord transactions are
# disabled on table (See transactional_mode in table options)".
#
# What this waits on is therefore itself: a prototype to build, a table declared
# transactional, and a measurement worth publishing.  Each is a change of its
# own, which is why the upgrade did not carry them.
#
# When it is built, it is to be built from a public git repository pinned by
# revision, not from a local checkout: a reader of this repository must be able
# to fetch the same source.
FROM eclipse-temurin:17-jre
WORKDIR /app
# COPY the built artifact here, once there is one to build.
# COPY build/libs/accord-sql.jar /app/app.jar
EXPOSE 9040
ENV LISTEN_PORT=9040
CMD ["bash","-lc","echo 'Accord SQL is a placeholder; see the comment in this Dockerfile.'; tail -f /dev/null"]
