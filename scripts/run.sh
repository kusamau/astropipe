#!/bin/sh
# Builds Astropipe if it has not been built yet, then runs it.
#
#   scripts/run.sh [path/to/config.json]     (default: ./config.json)
#
# Deliberately POSIX, with no bashisms, so that `sh scripts/run.sh` behaves
# exactly like `./scripts/run.sh`.
set -eu

cd "$(dirname "$0")/.."

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "run.sh: $1 is required but is not on PATH" >&2
        exit 1
    }
}

require java
if [ ! -f target/astropipe.jar ]; then
    require mvn
    echo "run.sh: target/astropipe.jar not found, building it first"
    mvn -q clean package -DskipTests
fi

# The source directory has to exist before startup; the archive is created if missing.
mkdir -p data/source data/archive logs

exec java -jar target/astropipe.jar "${1:-config.json}"
