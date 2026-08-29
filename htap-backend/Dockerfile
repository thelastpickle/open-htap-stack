# The build context is the repository root, not this directory: the module is one leaf of a
# Maven reactor and needs the parent pom and htap-common beside it.  Compose names the
# context and this file separately; .dockerignore at the root keeps the working tree out.
FROM maven:3.9-eclipse-temurin-25 AS build

WORKDIR /src

# Every pom first, so a source-only change reuses the dependency download below.  Every
# module's, and not only the three this one builds: the parent lists them all in <modules>,
# so a missing pom fails the reactor read even for a `-pl` build of one leaf.
COPY pom.xml ./
COPY htap-common/pom.xml htap-common/
COPY htap-cqlite/pom.xml htap-cqlite/
COPY htap-backend/pom.xml htap-backend/
COPY htap-sink/pom.xml htap-sink/
COPY htap-producer/pom.xml htap-producer/

# Not every artifact a Quarkus build asks for: this resolves the declared dependencies, and
# the augmentation step below fetches what it needs beyond them.  The value is the layer,
# which turns a rebuild after a code change from a full download into a compile.
#
# -C on both invocations, as the reactor and CI use: it makes a dependency checksum mismatch
# fail, where Maven's default is to warn and carry on with the bytes it got.  This build is
# the one that produces the deployed artefact, so it is the last place to relax that.
RUN mvn -B -ntp -C -pl htap-backend -am dependency:go-offline

COPY htap-common/src htap-common/src
COPY htap-cqlite/src htap-cqlite/src
COPY htap-backend/src htap-backend/src

# Tests are run by the reactor and by CI, not by an image build: a test here would need the
# services this image is built to talk to, and the cqlite tests would need the library the
# runtime stage below installs.
RUN mvn -B -ntp -C -pl htap-backend -am -DskipTests package

# The reader arrives prebuilt, one gzipped cdylib per architecture, and nothing here compiles;
# htap-cqlite/dist/VENDOR.md names the fork commit each was built from.  Unpacked in a stage of
# its own so the final image carries the one library and neither archive: a COPY of both would
# add 78 MB that no later RUN can take out again.
FROM maven:3.9-eclipse-temurin-25 AS cqlite

ARG CQLITE_VERSION=0.1.0

# The checksum is verified although the file is copied rather than fetched: the library is
# stored in Git LFS, and a checkout whose LFS objects did not arrive leaves a small pointer
# file in its place.  Failing here beats an image that starts and then declines every cqlite
# query with an unsatisfied link.
#
# `uname -m` names the file, so no TARGETARCH mapping is needed: this stage and the runtime
# stage are the same architecture, and a platform whose library dist/ does not hold fails on the
# missing file rather than installing the wrong one.  The gzipped form is what is committed,
# 38 MB for aarch64 and 40 MB for x86_64, so the install is a gunzip.
COPY htap-cqlite/dist/*.so.gz htap-cqlite/dist/*.so.gz.sha256 /tmp/cqlite/
RUN cd /tmp/cqlite \
 && LIB="libcqlite_datafusion_c-${CQLITE_VERSION}-linux-$(uname -m).so.gz" \
 && if [ ! -f "${LIB}" ]; then \
      echo "No cqlite library for $(uname -m).  htap-cqlite/dist/ holds:" >&2; ls >&2; \
      echo "Build one: see htap-cqlite/dist/VENDOR.md" >&2; \
      echo "If the file is there but small, its Git LFS object did not arrive." >&2; \
      exit 1; \
    fi \
 && sha256sum -c "${LIB}.sha256" \
 && mkdir -p /opt/htap/lib \
 && gunzip -c "${LIB}" > /opt/htap/lib/libcqlite_datafusion_c.so \
 && chmod 0555 /opt/htap/lib/libcqlite_datafusion_c.so

# glibc rather than Alpine's musl: the cqlite reader is a Rust cdylib built against glibc and
# loaded into this process through Panama.
FROM eclipse-temurin:25-jre

WORKDIR /app

# Where CqliteSettings looks for it by default, so nothing has to name the path.
COPY --from=cqlite /opt/htap/lib/ /opt/htap/lib/

# The fast-jar layout in dependency order: lib/ changes with the poms, app/ with the code, so
# the two are separate layers.
COPY --from=build /src/htap-backend/target/quarkus-app/lib/ ./lib/
COPY --from=build /src/htap-backend/target/quarkus-app/*.jar ./
COPY --from=build /src/htap-backend/target/quarkus-app/app/ ./app/
COPY --from=build /src/htap-backend/target/quarkus-app/quarkus/ ./quarkus/

EXPOSE 8000

# Three flags, each answering a different mechanism, and the same three the htap-cqlite tests
# run under; that pom's surefire comment records what each was measured against.
#
# enable-native-access covers both native surfaces at once: the Panama downcalls into the
# cqlite library, arrow-c-data's own System.load of its JNI shim, and the Cassandra driver's
# JNR clock.  illegal-native-access=deny now, because JDK 26 makes deny the default and a
# warning nobody reads is how that becomes a surprise later.  And add-opens java.base/java.nio
# is what arrow-memory-core needs to reach DirectByteBuffer's private constructor: without it
# the first cqlite result batch fails at MemoryUtil.directBuffer.
CMD ["java", \
     "--enable-native-access=ALL-UNNAMED", \
     "--illegal-native-access=deny", \
     "--add-opens=java.base/java.nio=ALL-UNNAMED", \
     "-jar", "quarkus-run.jar"]
