#!/usr/bin/env bash
# Deletes every stream on the local NATS server, after asking.
set -eu
nats stream ls
read -r -p "Delete ALL of these streams? (yes/no) " reply
[ "$reply" = yes ] || { echo "Aborted."; exit 0; }
nats stream ls -n | xargs -r -n1 nats stream rm -f
