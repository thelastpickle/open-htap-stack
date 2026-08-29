# The build context is the repository root, not this directory: the module is one leaf of a
# Maven reactor and needs the parent pom and htap-common beside it.  Compose names the
# context and this file separately; .dockerignore at the root keeps the working tree out.
FROM maven:3.9-eclipse-temurin-25 AS build

WORKDIR /src

# Every pom first, so a source-only change reuses the dependency download below.  htap-cqlite
# is listed as a module in the parent, so its pom has to be here for the reactor to load even
# though -pl/-am never builds it.
COPY pom.xml ./
COPY htap-common/pom.xml htap-common/
COPY htap-cqlite/pom.xml htap-cqlite/
COPY htap-backend/pom.xml htap-backend/

# Not every artifact a Quarkus build asks for: this resolves the declared dependencies, and
# the augmentation step below fetches what it needs beyond them.  The value is the layer,
# which turns a rebuild after a code change from a full download into a compile.
#
# -C on both invocations, as the reactor and CI use: it makes a dependency checksum mismatch
# fail, where Maven's default is to warn and carry on with the bytes it got.  This build is
# the one that produces the deployed artefact, so it is the last place to relax that.
RUN mvn -B -ntp -C -pl htap-backend -am dependency:go-offline

COPY htap-common/src htap-common/src
COPY htap-backend/src htap-backend/src

# Tests are run by the reactor and by CI, not by an image build: a test here would need the
# services this image is built to talk to.
RUN mvn -B -ntp -C -pl htap-backend -am -DskipTests package

# glibc rather than Alpine's musl: the cqlite reader this backend is growing towards is a Rust
# cdylib built against glibc and loaded into this process through Panama.
FROM eclipse-temurin:25-jre

WORKDIR /app

# The fast-jar layout in dependency order: lib/ changes with the poms, app/ with the code, so
# the two are separate layers.
COPY --from=build /src/htap-backend/target/quarkus-app/lib/ ./lib/
COPY --from=build /src/htap-backend/target/quarkus-app/*.jar ./
COPY --from=build /src/htap-backend/target/quarkus-app/app/ ./app/
COPY --from=build /src/htap-backend/target/quarkus-app/quarkus/ ./quarkus/

EXPOSE 8001

# The Cassandra driver loads a native library through JNR for its microsecond clock, which
# Java 25 warns about and a later release will refuse.  Granted here rather than left as a
# warning, because the cqlite reader will need the same permission for Panama.
CMD ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "quarkus-run.jar"]
