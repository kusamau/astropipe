#!/usr/bin/env bash
# Builds Astropipe if needed and runs it against ./config.json.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f target/astropipe.jar ] || mvn -q clean package -DskipTests
mkdir -p data/source data/archive logs
exec java -jar target/astropipe.jar "${1:-config.json}"
