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

# Detect seed files with pure shell (glob + case): the minio/mc image ships no
# `find`, so a $(find ...) check silently returns empty and skips the upload.
seed_has_files=""
for f in /seed_data/*; do
  [ -e "$f" ] || continue # glob matched nothing: $f is the literal pattern
  case "${f##*/}" in .gitkeep | README.md) continue ;; esac
  seed_has_files=1
  break
done

if [ -n "$seed_has_files" ]; then
  # mirror instead of cp: mc cp has no --exclude, and we don't want the folder's
  # README.md/.gitkeep ending up in the landing bucket alongside the data files.
  # The leading * is required: this mc release matches exclude patterns against
  # the object path with a leading slash ("/README.md").
  mc mirror --overwrite --exclude "*README.md" --exclude "*.gitkeep" /seed_data/ local/landing/
  echo "seed_data uploaded to s3://landing/"
else
  echo "no seed_data files yet -- landing bucket created empty"
fi

echo "MinIO warehouse (bronze/silver/gold) + landing buckets ready"
