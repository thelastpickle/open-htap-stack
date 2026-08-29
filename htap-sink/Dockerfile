# The build context is the repository root, not this directory: the module is one leaf of a
# Maven reactor and needs the parent pom and htap-common beside it.  Compose names the
# context and this file separately; .dockerignore at the root keeps the working tree out.
FROM maven:3.9-eclipse-temurin-25 AS build

WORKDIR /src

# Every module's pom, not only this module's: the parent lists all of them in <modules>,
# so Maven cannot read the reactor at all if one is missing, even for a `-pl` build of one
# leaf.  What `-pl … -am` then limits is which of them is built.
COPY pom.xml ./
COPY htap-common/pom.xml htap-common/
COPY htap-cqlite/pom.xml htap-cqlite/
COPY htap-backend/pom.xml htap-backend/
COPY htap-sink/pom.xml htap-sink/
COPY htap-producer/pom.xml htap-producer/

# -C as the reactor and CI use, so a dependency checksum mismatch fails rather than warning
# and carrying on with the bytes it got.
RUN mvn -B -ntp -C -pl htap-sink -am dependency:go-offline

COPY htap-common/src htap-common/src
COPY htap-sink/src htap-sink/src

# Tests are run by the reactor and by CI, not by an image build.
RUN mvn -B -ntp -C -pl htap-sink -am -DskipTests package

FROM eclipse-temurin:25-jre

WORKDIR /app

# The jar and its dependencies in dependency order: lib/ changes with the poms and the jar with
# the code, so the two are separate layers.  The manifest names lib/ as its class path, which is
# why the two must land beside each other.
COPY --from=build /src/htap-sink/target/lib/ ./lib/
COPY --from=build /src/htap-sink/target/htap-sink-*.jar ./htap-sink.jar

# enable-native-access is not cosmetic here: without it the driver's JNR clock and its shaded
# Guava each print a WARNING about sun.misc.Unsafe and System::load at every start, four lines
# that would sit in front of every log this container writes.  illegal-native-access=deny for
# the reason the backend's image gives: JDK 26 makes deny the default, and a warning nobody
# reads is how that becomes a surprise later.
CMD ["java", \
     "--enable-native-access=ALL-UNNAMED", \
     "--illegal-native-access=deny", \
     "-jar", "htap-sink.jar"]
