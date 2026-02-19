#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
OUT_FILE="$HOME/.s3cfg"

ak="$(grep -E '^S3_ACCESS_KEY=' "$ENV_FILE" | tail -n1 | cut -d= -f2- | tr -d '\r' | xargs)"
sk="$(grep -E '^S3_SECRET_KEY=' "$ENV_FILE" | tail -n1 | cut -d= -f2- | tr -d '\r' | xargs)"

cat > "$OUT_FILE" <<EOF
[default]
access_key = ${ak}
secret_key = ${sk}

bucket_location = ru-central1
host_base = storage.yandexcloud.net
host_bucket = %(bucket)s.storage.yandexcloud.net

use_https = True
signature_v2 = False
EOF

chmod 600 "$OUT_FILE"
echo "Wrote $OUT_FILE"
