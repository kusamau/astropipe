#!/bin/sh
# The source directory has to exist before startup; the archive is created if missing.
mkdir -p data/source data/archive logs

java -jar target/astropipe.jar config.json
