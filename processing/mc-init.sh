#!/bin/sh
# One-shot MinIO bootstrap: warehouse bucket (bronze/silver/gold prefixes) +
# landing bucket, seeded from ./seed_data if Tama has dropped raw batch files
# there. Runs once via the mc-init service in processing/docker-compose.yml.
set -eu

until mc alias set local http://minio:9000 admin password12345; do
  echo "waiting for minio..."
  sleep 2
done

mc mb --ignore-existing local/warehouse
# mc refuses /dev/null as a cp source, so create the empty layer markers via
# mc pipe (zero-byte object from empty stdin) instead
mc pipe local/warehouse/bronze/.keep </dev/null
mc pipe local/warehouse/silver/.keep </dev/null
mc pipe local/warehouse/gold/.keep </dev/null

mc mb --ignore-existing local/landing

if [ -n "$(find /seed_data -type f ! -name .gitkeep ! -name README.md 2>/dev/null)" ]; then
  mc cp --recursive /seed_data/ local/landing/
  echo "seed_data uploaded to s3://landing/"
else
  echo "no seed_data files yet -- landing bucket created empty"
fi

echo "MinIO warehouse (bronze/silver/gold) + landing buckets ready"
