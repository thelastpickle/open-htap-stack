# A placeholder for Accord SQL, the Postgres-dialect SQL prototype over Accord
# transactions.  Nothing in podman-compose.yml references this image, so nothing
# starts it; the directory holds the decisions taken about it, not a build.
#
# It is deferred, and the reason is Cassandra's version.  Accord needs 6.0, and
# 6.0 writes BTI SSTables at version "ea": cqlite reads na, nb, oa and da only,
# and cassandra-analytics has bridges for 4.0 and 5.0 only.  Moving to 6.0
# therefore takes the cqlite and spark_bulk paths with it, which are two of the
# five the demo exists to compare.  No setting holds BTI at da; BtiFormat's
# current_version is a constant, and only the big format's version follows
# storage_compatibility_mode.  So this waits on a reader for ea.
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
