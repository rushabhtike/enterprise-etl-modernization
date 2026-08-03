#!/usr/bin/env bash

set -euo pipefail

WAREHOUSE_PATH="/home/hadoop/workspace/docker/glue/output/iceberg"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <job-path> [job arguments...]"
    exit 1
fi

JOB_PATH="$1"
shift

if [[ ! -f "$JOB_PATH" ]]; then
    echo "Job file not found: $JOB_PATH"
    exit 1
fi

exec spark-submit \
    --master "local[4]" \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse="$WAREHOUSE_PATH" \
    "$JOB_PATH" \
    "$@"