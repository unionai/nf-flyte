#!/bin/sh
# Vendors the flyteidl2 protos the controller uses, with everything they import, into
# controller/src/main/proto. Usage: scripts/update-protos.sh [flyte git ref, default: FLYTE_REF]
set -e
FLYTE_REF=${1:-v2.0.52}
out=$(dirname "$0")/../controller/src/main/proto

rm -rf "$out"
buf export "https://github.com/flyteorg/flyte.git#tag=$FLYTE_REF" \
    --path flyteidl2/actions/actions_service.proto \
    --path flyteidl2/workflow/run_service.proto \
    --path flyteidl2/auth/auth_service.proto \
    --path flyteidl2/task/common.proto \
    --path flyteidl2/dataproxy/dataproxy_service.proto \
    -o "$out"
echo "$FLYTE_REF" > "$out/FLYTE_REF"
