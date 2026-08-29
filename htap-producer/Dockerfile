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
RUN mvn -B -ntp -C -pl htap-producer -am dependency:go-offline

COPY htap-common/src htap-common/src
COPY htap-producer/src htap-producer/src

# Tests are run by the reactor and by CI, not by an image build.
RUN mvn -B -ntp -C -pl htap-producer -am -DskipTests package

FROM eclipse-temurin:25-jre

WORKDIR /app

# A small curated corpus spanning several unrelated subjects.  It is the default because the
# dashboard's vector search only says anything if the snippets differ in meaning.
COPY htap-producer/wikipedia.txt /app/wikipedia.txt

# enwik9 is 322 MB from one small host, and it is here only for volume testing
# (TEXT_FILE=/app/enwik9), so the fetch is off by default:
#   podman compose build --build-arg FETCH_ENWIK9=1 data-producer
#
# It was unconditional, and it failed CI.  wget wrote a file in 150 ms and exited 0, where
# the same step had taken 13.7 s and succeeded six hours earlier, so the host answered that
# runner with something short and the build died one step later at "unzip: cannot find
# zipfile directory in one of enwik9.zip".  -q had discarded whatever the server said, and no
# checksum caught the difference.  Three build attempts could not help either, because the bad
# download sat in a layer the second and third reused.
#
# So: one RUN, so a bad download leaves no layer behind; a checksum, so a short body fails
# where it happens rather than at the unzip; and -c dropped, since continuing a file that may
# be a block page is how the two get concatenated.  The archive has not changed since
# 2023-02-24 and is 322,592,222 bytes.
ARG FETCH_ENWIK9=0
ARG ENWIK9_SHA256=99cdb5ac84392252d3f0912ccedd195bc95bd80cbef3b0cdf2eee4ad9a3b7a51
RUN if [ "${FETCH_ENWIK9}" = "1" ]; then \
      apt-get -o Acquire::Retries=5 update && apt-get install -y --no-install-recommends wget unzip \
      && wget -q --show-progress --https-only --tries=5 --waitretry=15 \
              --retry-connrefused --timeout=60 https://mattmahoney.net/dc/enwik9.zip \
      && echo "${ENWIK9_SHA256}  enwik9.zip" | sha256sum -c - \
      && unzip -q enwik9.zip \
      && rm enwik9.zip \
      && rm -rf /var/lib/apt/lists/*; \
    fi

# The one setting that names a path inside this image rather than something an operator tunes,
# so it is here and the rest are in compose.
ENV TEXT_FILE="/app/wikipedia.txt"

# The jar and its dependencies in dependency order: lib/ changes with the poms and the jar with
# the code.  The manifest names lib/ as its class path, so the two must land beside each other.
COPY --from=build /src/htap-producer/target/lib/ ./lib/
COPY --from=build /src/htap-producer/target/htap-producer-*.jar ./htap-producer.jar

# enable-native-access for the reason the sink's image gives: without it the Kafka client's
# lz4 and its own checksum code print a warning about System::load in front of every log.
CMD ["java", \
     "--enable-native-access=ALL-UNNAMED", \
     "--illegal-native-access=deny", \
     "-jar", "htap-producer.jar"]
