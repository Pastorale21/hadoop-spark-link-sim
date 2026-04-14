#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

HDFS_INPUT_DIR="${HDFS_INPUT_DIR:-/user/${USER}/linksim/input}"
HDFS_INPUT_PATH="${HDFS_INPUT_PATH:-hdfs:///user/${USER}/linksim/input/edges_sample.txt}"
HDFS_OUTPUT_PATH="${HDFS_OUTPUT_PATH:-hdfs:///user/${USER}/linksim/output/run_sample}"
LOCAL_SAMPLE_INPUT="${LOCAL_SAMPLE_INPUT:-${PROJECT_ROOT}/data/sample/edges_sample.txt}"

hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
hdfs dfs -put -f "${LOCAL_SAMPLE_INPUT}" "${HDFS_INPUT_DIR}/edges_sample.txt"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files "${PROJECT_ROOT}/src/utils.py,${PROJECT_ROOT}/src/preprocess.py,${PROJECT_ROOT}/src/similarity.py,${PROJECT_ROOT}/src/topk.py" \
  "${PROJECT_ROOT}/src/main.py" \
  --master yarn \
  --input "${HDFS_INPUT_PATH}" \
  --output "${HDFS_OUTPUT_PATH}" \
  --topk 2 \
  --write-intermediate
