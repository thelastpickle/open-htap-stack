#!/usr/bin/env bash
#
# Ten checks a machine must pass before this stack will come up, each with its own remedy.
#
#   scripts/setup-preflight.sh          # checks only; downloads nothing
#   scripts/setup-preflight.sh --pull   # also pulls the four upstream images the compose file names
#
# Run it on a machine that has not run the stack before, and again after a change to
# podman-compose.yml that adds a service, a published port or an upstream image.
#
# Three of its lists are read from `compose config` rather than repeated here, so that a
# service added to that file is checked without a second edit: the published host ports, the
# container names, and the upstream images.  Reading them from the merged config also honours
# .env, so a moved ACCORD_SQL_PORT or BACKEND_PORT is the port this checks.  Each list has a
# hardcoded fallback for a machine whose runtime cannot answer `config`, and those fallbacks
# are the only drift to watch.
#
# What this does not check is the build itself.  Six of the ten services are built from
# source here, and a build failure is loud and fixable at the time; every check below is for
# something that fails quietly, hours later, or in a way that reads as corruption.  The
# images are also published, one per service, at
# ghcr.io/thelastpickle/open-htap-stack/<service>:latest for anyone who would rather not
# build; :latest is built from trunk, and a branch has its own tag.
#
# `set -e` is deliberately absent.  A person needs every failure in one run rather than the
# first one: a machine short of memory is usually short of disk as well, and two visits to
# the podman machine settings are one visit too many.
set -uo pipefail

REQUIRED_MEM_BYTES=12884901888   # 12 GiB.  The declared ceilings sum above it: 8g on
                                 # cassandra and on backend, 2g on accord-sql, Presto's heap
                                 # and three Spark JVMs beside them.
REQUIRED_CPUS=6                  # A warning, not a failure.  The Spark entrypoint gives the
                                 # Thrift Server nproc-2 above four cores and half the box
                                 # below, so the stack starts on four and every scan is slower.
REQUIRED_DISK_GB=40
PREFERRED_COMPOSE="2.24.4"       # The release that understands the !override tag, which a
                                 # local override file uses to drop a published port.  A
                                 # warning, because podman-compose.yml alone needs no override.
SUBNET="172.20.0."               # Pinned in podman-compose.yml, as is cassandra's 172.20.0.10,
                                 # so a network already holding the block cannot be worked
                                 # around by changing one of the two.

# The fallbacks, used only when `compose config` does not answer.  Derived from
# podman-compose.yml; a service added there belongs in all three.
PORTS_FALLBACK="4000 4040 5432 7077 8000 8080 8081 8085 8088 9042 9043 9092 10000"
NAMES_FALLBACK="kafka apicurio cassandra spark presto accord-sql data-producer data-cassandra-sink backend frontend"
IMAGES_FALLBACK="apache/kafka:4.3.1 apicurio/apicurio-registry:3.0.13 prestodb/presto:0.298.1 apache/spark:3.5.9-scala2.12-java17-python3-ubuntu"

DO_PULL="no"
while [ $# -gt 0 ]; do
  case "$1" in
    --pull) DO_PULL="yes"; shift ;;
    -h|--help) sed -n '2,6p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1"; exit 2 ;;
  esac
done

PASSED=0
WARNED=0
FAILED=0

pass() { PASSED=$((PASSED + 1)); printf 'PASS  %s\n' "$1"; }
warn() { WARNED=$((WARNED + 1)); printf 'WARN  %s\n' "$1"; [ $# -gt 1 ] && printf '      %s\n' "$2"; return 0; }
fail() { FAILED=$((FAILED + 1)); printf 'FAIL  %s\n' "$1"; [ $# -gt 1 ] && printf '      remedy: %s\n' "$2"; return 0; }

cd "$(dirname "$0")/.." || exit 2

printf '\n=== open-htap-stack setup pre-flight ===\n\n'

# ── 1. The architecture ──────────────────────────────────────────────────────────────
# The backend image installs a prebuilt cqlite wheel chosen by `uname -m`, and there is a
# wheel for two architectures only.  Nothing later in this script can substitute for the
# right machine.
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)   READER_ARCH="x86_64" ;;
  aarch64|arm64)  READER_ARCH="aarch64" ;;
  *)              READER_ARCH="" ;;
esac
if [ -n "$READER_ARCH" ]; then
  pass "architecture $ARCH has a cqlite wheel"
else
  fail "architecture $ARCH has no cqlite wheel" \
       "the stack runs on x86_64 and on arm64 only; see backend/dist/VENDOR.md to build a third"
fi

# ── 2. The container runtime ─────────────────────────────────────────────────────────
# podman first, because that is what podman-compose.yml is named for and what CI runs; a
# docker that answers is accepted, and the rest of the script asks whichever answered.
CLI=""
CLI_SEEN=""
for candidate in podman docker; do
  command -v "$candidate" > /dev/null 2>&1 || continue
  CLI_SEEN="${CLI_SEEN} ${candidate}"
  if [ -z "$CLI" ] && "$candidate" info > /dev/null 2>&1; then CLI="$candidate"; fi
done
if [ -n "$CLI" ]; then
  pass "$CLI answers: $($CLI --version 2>/dev/null | head -1)"
elif [ -n "$CLI_SEEN" ]; then
  fail "${CLI_SEEN# } is installed but does not answer" \
       "run 'podman machine start', or start Docker Desktop and wait for it to settle"
else
  fail "neither podman nor docker is on the PATH" \
       "install podman 5 or newer, or Docker Desktop 4.38 or newer"
fi

# ── 3. Compose ───────────────────────────────────────────────────────────────────────
# `podman compose` delegates to an external provider and prints a banner about it on stderr,
# so --short's own line is taken from stdout alone.
COMPOSE=""
COMPOSE_VERSION=""
if [ -n "$CLI" ] && $CLI compose version > /dev/null 2>&1; then
  COMPOSE="$CLI compose"
  COMPOSE_VERSION="$($CLI compose version --short 2>/dev/null | tail -1)"
elif command -v docker-compose > /dev/null 2>&1; then
  COMPOSE="docker-compose"
  COMPOSE_VERSION="$(docker-compose version --short 2>/dev/null | tail -1)"
fi
if [ -z "$COMPOSE" ]; then
  fail "no compose command answers" \
       "install the compose plugin, or podman-compose; podman delegates to whichever it finds"
else
  # Sort the two versions and see which comes first.  `sort -V` is in coreutils and in macOS
  # 13 and later; where it is absent this reports the version rather than guessing.
  if printf '%s\n%s\n' "$PREFERRED_COMPOSE" "${COMPOSE_VERSION#v}" | sort -V -c > /dev/null 2>&1; then
    pass "$COMPOSE ${COMPOSE_VERSION} understands the !override tag"
  else
    warn "$COMPOSE ${COMPOSE_VERSION} is older than ${PREFERRED_COMPOSE}" \
         "podman-compose.yml alone runs, but an override file using the !override tag needs ${PREFERRED_COMPOSE}"
  fi
fi

# ── 4. The compose file, merged ──────────────────────────────────────────────────────
# One check that catches several things at once: a stale checkout, a compose too old to read
# the file, and compose.yml degraded to a text file by a clone without symbolic links.  The
# merged output is also where the next three checks get their lists.
CONFIG=""
if [ ! -L compose.yml ] && [ "$(wc -c < compose.yml 2>/dev/null | tr -d ' ')" -lt 100 ]; then
  fail "compose.yml is a short text file rather than a symbolic link to podman-compose.yml" \
       "clone again with symbolic links enabled; on Windows, inside WSL2 on the Linux filesystem"
fi
if [ -n "$COMPOSE" ]; then
  CONFIG="$($COMPOSE -f podman-compose.yml config 2>/dev/null)"
  if [ -n "$CONFIG" ]; then
    pass "podman-compose.yml parses, $(printf '%s\n' "$CONFIG" | sed -n 's/^ *container_name: *//p' | wc -l | tr -d ' ') services"
  else
    fail "podman-compose.yml does not parse" \
         "run: $COMPOSE -f podman-compose.yml config"
  fi
fi

# ── 5. Memory and cores ──────────────────────────────────────────────────────────────
# On macOS both figures are the virtual machine's rather than the laptop's, which is the
# number that matters: podman 6.0.2 reports Host.MemTotal as the machine's memory.
MEM_BYTES=""
NCPU=""
if [ "$CLI" = "podman" ]; then
  MEM_BYTES="$(podman info --format '{{.Host.MemTotal}}' 2>/dev/null)"
  NCPU="$(podman info --format '{{.Host.CPUs}}' 2>/dev/null)"
elif [ "$CLI" = "docker" ]; then
  MEM_BYTES="$(docker info --format '{{.MemTotal}}' 2>/dev/null)"
  NCPU="$(docker info --format '{{.NCPU}}' 2>/dev/null)"
fi
if [ -z "$MEM_BYTES" ]; then
  warn "could not read the memory the runtime has" \
       "podman: 'podman machine inspect --format {{.Resources.Memory}}', in MiB"
elif [ "$MEM_BYTES" -ge "$REQUIRED_MEM_BYTES" ]; then
  pass "the runtime has $((MEM_BYTES / 1073741824)) GiB of memory"
else
  fail "the runtime has $((MEM_BYTES / 1073741824)) GiB of memory, and the stack needs 12" \
       "podman: 'podman machine stop && podman machine set --memory 12288 && podman machine start'; Docker Desktop: Settings, Resources, Memory"
fi
if [ -z "$NCPU" ]; then
  warn "could not read the cores the runtime has"
elif [ "$NCPU" -ge "$REQUIRED_CPUS" ]; then
  pass "the runtime has ${NCPU} cores"
else
  warn "the runtime has ${NCPU} cores, and ${REQUIRED_CPUS} is comfortable" \
       "the stack runs on four, more slowly; podman machine set --cpus 6"
fi

# ── 6. Disk, in the two places it runs out ───────────────────────────────────────────
# `df` on the host does not see the image layers, which live inside the virtual machine, so
# the two are reported separately.  cassandra-data/ grows on the host while the stack runs;
# the images and their layers grow inside the machine.
DISK_FREE_GB="$(df -g . 2>/dev/null | awk 'NR==2 {print $4}')"
if [ -z "$DISK_FREE_GB" ]; then
  DISK_FREE_GB="$(df -BG . 2>/dev/null | awk 'NR==2 {gsub(/G/,"",$4); print $4}')"
fi
if [ -z "$DISK_FREE_GB" ]; then
  warn "could not read the free space on this filesystem"
elif [ "$DISK_FREE_GB" -ge "$REQUIRED_DISK_GB" ]; then
  pass "${DISK_FREE_GB} GB free where the repository sits"
else
  fail "${DISK_FREE_GB} GB free where the repository sits, and the stack needs ${REQUIRED_DISK_GB}" \
       "cassandra-data/ grows while the stack runs; free space, or move the clone"
fi

VM_DISK_GB=""
if [ "$CLI" = "podman" ]; then
  VM_DISK_GB="$(podman machine inspect --format '{{.Resources.DiskSize}}' 2>/dev/null | head -1)"
else
  for candidate in \
    "${HOME}/Library/Group Containers/group.com.docker/settings-store.json" \
    "${HOME}/Library/Group Containers/group.com.docker/settings.json" \
    "${HOME}/.docker/desktop/settings-store.json" \
    "${HOME}/.docker/desktop/settings.json"; do
    [ -f "$candidate" ] || continue
    command -v python3 > /dev/null 2>&1 || break
    VM_DISK_GB="$(python3 -c 'import json,sys
d = json.load(open(sys.argv[1]))
mib = d.get("diskSizeMiB") or d.get("DiskSizeMiB") or 0
print(int(mib) // 1024)' "$candidate" 2>/dev/null)"
    break
  done
fi
if [ -z "${VM_DISK_GB:-}" ] || [ "${VM_DISK_GB:-0}" -eq 0 ] 2>/dev/null; then
  warn "could not read the virtual disk ceiling" \
       "ignore this on a Linux host, where there is no virtual machine; otherwise read it in the runtime's settings"
elif [ "$VM_DISK_GB" -ge "$REQUIRED_DISK_GB" ]; then
  pass "the virtual disk may grow to ${VM_DISK_GB} GB"
else
  fail "the virtual disk stops at ${VM_DISK_GB} GB, and the images need ${REQUIRED_DISK_GB}" \
       "podman machine set --disk-size 100, or Docker Desktop, Settings, Resources, Virtual disk limit"
fi

# ── 7. The published host ports ──────────────────────────────────────────────────────
# Every service also sets container_name, so a second copy of this stack collides by name as
# well, which the runtime reports clearly.  A port held by something else is the quiet one.
PORTS="$(printf '%s\n' "$CONFIG" | sed -n 's/^ *published: "\([0-9]*\)".*/\1/p' | sort -un | tr '\n' ' ')"
[ -z "$(printf '%s' "$PORTS" | tr -d ' ')" ] && PORTS="$PORTS_FALLBACK"
NAMES="$(printf '%s\n' "$CONFIG" | sed -n 's/^ *container_name: *//p' | tr '\n' ' ')"
[ -z "$(printf '%s' "$NAMES" | tr -d ' ')" ] && NAMES="$NAMES_FALLBACK"
BUSY=""
for port in $PORTS; do
  if command -v lsof > /dev/null 2>&1; then
    owner="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -F c 2>/dev/null | sed -n 's/^c//p' | head -1)"
  else
    owner=""
    nc -z 127.0.0.1 "$port" > /dev/null 2>&1 && owner="something"
  fi
  [ -n "$owner" ] && BUSY="${BUSY} ${port}(${owner})"
done
# This stack's own containers hold every one of those ports while it runs, and on macOS they
# all read as one forwarding process, so the owner name cannot tell the two apart.  Asking
# whether the stack is up can: a running stack is not a collision.
STACK_UP=0
if [ -n "$CLI" ]; then
  for name in $NAMES; do
    $CLI ps --format '{{.Names}}' 2>/dev/null | grep -qx "$name" && STACK_UP=$((STACK_UP + 1))
  done
fi
if [ -z "$BUSY" ]; then
  pass "all $(printf '%s\n' $PORTS | wc -l | tr -d ' ') published ports are free"
elif [ "$STACK_UP" -gt 0 ]; then
  warn "this stack is already running, ${STACK_UP} containers, and holds its own ports:${BUSY}" \
       "nothing to do; run '$COMPOSE -f podman-compose.yml down' first to check from scratch"
else
  fail "these published ports are in use:${BUSY}" \
       "stop what holds each one; 5432, 8000 and 8080 are the usual three, and .env moves 5432"
fi

# ── 8. The 172.20.0.0/16 subnet ──────────────────────────────────────────────────────
# podman exposes a network's blocks as .Subnets and docker as .IPAM.Config, so both forms are
# asked and the empty answer decides which runtime this is.
OVERLAP=""
if [ -n "$CLI" ]; then
  for net in $($CLI network ls --format '{{.Name}}' 2>/dev/null); do
    case "$net" in htap-net|*_htap-net) continue ;; esac
    subnets="$($CLI network inspect "$net" --format '{{range .Subnets}}{{.Subnet}} {{end}}' 2>/dev/null)"
    [ -z "$subnets" ] && subnets="$($CLI network inspect "$net" --format '{{range .IPAM.Config}}{{.Subnet}} {{end}}' 2>/dev/null)"
    case "$subnets" in *"$SUBNET"*) OVERLAP="${OVERLAP} ${net}" ;; esac
  done
  if [ -z "$OVERLAP" ]; then
    pass "no other network holds ${SUBNET}0/16"
  else
    fail "these networks hold ${SUBNET}0/16:${OVERLAP}" \
         "stop that project, or '$CLI network rm' the network if nothing needs it"
  fi
fi

# ── 9. Git LFS, and whether its objects arrived ──────────────────────────────────────
# A clone without LFS leaves pointer files of about 130 bytes, and every one of them is
# copied into an image by a build that then verifies a checksum against it.  So this is a
# failure and not a warning: the Sidecar tarball, the cqlite wheel for this architecture and
# the Spark offline ivy jars are each read by a build here.
if ! command -v git > /dev/null 2>&1; then
  fail "git is not on the PATH" "install git, then clone this repository again"
elif ! git lfs version > /dev/null 2>&1; then
  fail "git-lfs is not installed" "install git-lfs, then run 'git lfs pull' in this clone"
else
  POINTERS=""
  CANDIDATES="$(ls cassandra/dist/*.tar.gz 2>/dev/null)"
  [ -n "$READER_ARCH" ] && CANDIDATES="${CANDIDATES}
$(ls backend/dist/*linux_${READER_ARCH}.whl 2>/dev/null)"
  CANDIDATES="${CANDIDATES}
$(find spark/ivy/cache -name '*.jar' 2>/dev/null)"
  MISSING_ARTEFACT="no"
  [ -n "$READER_ARCH" ] && [ -z "$(ls backend/dist/*linux_${READER_ARCH}.whl 2>/dev/null)" ] && MISSING_ARTEFACT="yes"
  for f in $CANDIDATES; do
    [ -f "$f" ] || continue
    size="$(wc -c < "$f" | tr -d ' ')"
    [ "$size" -lt 1000 ] && POINTERS="${POINTERS} $(basename "$f")"
  done
  if [ "$MISSING_ARTEFACT" = "yes" ]; then
    fail "no cqlite wheel for linux_${READER_ARCH} in backend/dist/" \
         "the checkout is incomplete or predates this architecture; see backend/dist/VENDOR.md"
  elif [ -z "$POINTERS" ]; then
    pass "the Git LFS objects are real files"
  else
    # Printed short on purpose: the ivy cache holds about eighty jars and a clone without LFS
    # leaves every one of them a pointer, which is a screen of names saying one thing.
    COUNT="$(printf '%s\n' $POINTERS | wc -l | tr -d ' ')"
    fail "Git LFS objects still holding a pointer: ${COUNT}, among them$(printf '%s' "$POINTERS" | cut -d' ' -f1-3)" \
         "run 'git lfs pull'; a build copies these files and verifies a checksum against them"
  fi
fi

# ── 10. The clock ────────────────────────────────────────────────────────────────────
# event_bucket is derived from the clock, so a virtual machine that drifted across a laptop
# sleep computes a different current bucket from the one its own rows landed in, and the
# dashboard reports a window as open that has closed.
HOST_EPOCH="$(date -u +%s)"
VM_EPOCH=""
if [ -n "$CLI" ]; then
  RUNNING="$($CLI ps --format '{{.Names}}' 2>/dev/null | grep -Ex 'cassandra|backend' | head -1)"
  if [ -n "$RUNNING" ]; then
    VM_EPOCH="$($CLI exec "$RUNNING" date -u +%s 2>/dev/null)"
  else
    IMAGES="$(printf '%s\n' "$CONFIG" | sed -n 's/^ *image: *//p' | sort -u)"
    [ -z "$IMAGES" ] && IMAGES="$IMAGES_FALLBACK"
    for img in $IMAGES; do
      $CLI image inspect "$img" > /dev/null 2>&1 || continue
      VM_EPOCH="$($CLI run --rm --entrypoint /bin/sh "$img" -c 'date -u +%s' 2>/dev/null | tail -1)"
      [ -n "$VM_EPOCH" ] && break
    done
  fi
fi
if [ -z "$VM_EPOCH" ]; then
  warn "no running container and no local image to read the clock from" \
       "run this check again after the first 'up', or after --pull"
else
  SKEW=$((HOST_EPOCH - VM_EPOCH))
  [ "$SKEW" -lt 0 ] && SKEW=$((-SKEW))
  if [ "$SKEW" -le 5 ]; then
    pass "the host clock and the container clock agree, ${SKEW}s apart"
  else
    fail "the host clock and the container clock are ${SKEW}s apart" \
         "restart the machine: 'podman machine stop && podman machine start'; the clock drifts across a laptop sleep"
  fi
fi

# ── The pull, only when asked ────────────────────────────────────────────────────────
# The four upstream images the compose file names, and not the six built here: a build
# fetches its own bases, and pulling those would be this script guessing at Dockerfiles.
if [ "$DO_PULL" = "yes" ] && [ -n "$CLI" ]; then
  IMAGES="$(printf '%s\n' "$CONFIG" | sed -n 's/^ *image: *//p' | sort -u)"
  [ -z "$IMAGES" ] && IMAGES="$IMAGES_FALLBACK"
  printf '\nPulling the upstream images podman-compose.yml names.\n\n'
  PULL_FAILED=""
  for img in $IMAGES; do
    if $CLI pull "$img"; then :; else PULL_FAILED="${PULL_FAILED} ${img}"; fi
  done
  if [ -z "$PULL_FAILED" ]; then
    pass "the upstream images are on this machine"
  else
    fail "these pulls did not finish:${PULL_FAILED}" \
         "run it again; a resumed pull keeps the layers it already has"
  fi
fi

printf '\n%d passed, %d warnings, %d failed\n' "$PASSED" "$WARNED" "$FAILED"
if [ "$FAILED" -gt 0 ]; then
  printf 'Act on each FAIL line above.\n'
  exit 1
fi
printf 'This machine can run the stack.\n'
exit 0
