#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

spark-submit \
  --master local[*] \
  --py-files "${PROJECT_ROOT}/src/utils.py,${PROJECT_ROOT}/src/preprocess.py,${PROJECT_ROOT}/src/similarity.py,${PROJECT_ROOT}/src/topk.py" \
  "${PROJECT_ROOT}/src/main.py" \
  --master local[*] \
  --input "${PROJECT_ROOT}/data/sample/edges_sample.txt" \
  --output "${PROJECT_ROOT}/output/local_run" \
  --topk 2 \
  --write-intermediate
