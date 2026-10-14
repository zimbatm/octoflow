#!/usr/bin/env bash
#
# create-bucket.sh - Create $S3_BUCKET on the local rustfs server
#
# Only for local development. In production the bucket exists already and
# the s3-worker credential should not be allowed to create buckets.
#
set -euo pipefail

endpoint=${S3_ENDPOINT:-http://127.0.0.1:9000}

status=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
    --aws-sigv4 "aws:amz:${AWS_REGION:-us-east-1}:s3" \
    --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" \
    "${endpoint}/${S3_BUCKET}")

case $status in
200) echo "Bucket ${S3_BUCKET} is ready" ;;
409) echo "Bucket ${S3_BUCKET} exists" ;;
*)
    echo "Failed to create bucket ${S3_BUCKET}: HTTP ${status}" >&2
    exit 1
    ;;
esac
