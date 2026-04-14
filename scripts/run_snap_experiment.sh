#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DATASET_NAME="${DATASET_NAME:-snap-web-NotreDame}"
MASTER="${MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
TOPK="${TOPK:-20}"
MAX_REFERRERS_PER_DST="${MAX_REFERRERS_PER_DST:-0}"
WRITE_INTERMEDIATE="${WRITE_INTERMEDIATE:-0}"
SPARK_UI_ENABLED="${SPARK_UI_ENABLED:-false}"

SNAP_LOCAL_INPUT="${SNAP_LOCAL_INPUT:-${PROJECT_ROOT}/data/snap/web-NotreDame.txt}"
HDFS_INPUT_DIR="${HDFS_INPUT_DIR:-/user/${USER}/linksim/input/snap}"
SNAP_HDFS_INPUT_PATH="${SNAP_HDFS_INPUT_PATH:-hdfs:///user/${USER}/linksim/input/snap/web-NotreDame.txt}"

RUN_ID="${RUN_ID:-$(date '+%Y%m%d-%H%M%S')}"
RESULTS_ROOT="${RESULTS_ROOT:-${PROJECT_ROOT}/results}"
RUN_RECORD_DIR="${RESULTS_ROOT}/runs/${RUN_ID}_${DATASET_NAME}"
RUN_LOG="${RUN_RECORD_DIR}/spark_submit.log"
COMMAND_FILE="${RUN_RECORD_DIR}/command.txt"
METADATA_FILE="${RUN_RECORD_DIR}/metadata.txt"

mkdir -p "${RUN_RECORD_DIR}"

if [[ "${MASTER}" == "yarn" ]]; then
  INPUT_PATH="${INPUT_PATH:-${SNAP_HDFS_INPUT_PATH}}"
  OUTPUT_PATH="${OUTPUT_PATH:-hdfs:///user/${USER}/linksim/output/experiments/${DATASET_NAME}/${RUN_ID}}"
  UPLOAD_TO_HDFS="${UPLOAD_TO_HDFS:-1}"
else
  INPUT_PATH="${INPUT_PATH:-${SNAP_LOCAL_INPUT}}"
  OUTPUT_PATH="${OUTPUT_PATH:-${PROJECT_ROOT}/output/experiments/${DATASET_NAME}/${RUN_ID}}"
  UPLOAD_TO_HDFS="${UPLOAD_TO_HDFS:-0}"
fi

if [[ "${MASTER}" == "yarn" && "${UPLOAD_TO_HDFS}" == "1" ]]; then
  if [[ ! -f "${SNAP_LOCAL_INPUT}" ]]; then
    echo "SNAP local input not found: ${SNAP_LOCAL_INPUT}" >&2
    echo "Please download and extract web-NotreDame.txt first." >&2
    exit 1
  fi

  hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
  hdfs dfs -put -f "${SNAP_LOCAL_INPUT}" "${HDFS_INPUT_DIR}/$(basename "${SNAP_HDFS_INPUT_PATH}")"
fi

SPARK_CMD=(
  spark-submit
  --master "${MASTER}"
)

if [[ "${MASTER}" == "yarn" ]]; then
  SPARK_CMD+=(
    --deploy-mode "${DEPLOY_MODE}"
  )
fi

SPARK_CMD+=(
  --conf "spark.ui.enabled=${SPARK_UI_ENABLED}"
  --py-files "${PROJECT_ROOT}/src/utils.py,${PROJECT_ROOT}/src/preprocess.py,${PROJECT_ROOT}/src/similarity.py,${PROJECT_ROOT}/src/topk.py"
  "${PROJECT_ROOT}/src/main.py"
  --master "${MASTER}"
  --input "${INPUT_PATH}"
  --output "${OUTPUT_PATH}"
  --topk "${TOPK}"
  --max-referrers-per-dst "${MAX_REFERRERS_PER_DST}"
)

if [[ "${WRITE_INTERMEDIATE}" == "1" ]]; then
  SPARK_CMD+=(--write-intermediate)
fi

printf '%q ' "${SPARK_CMD[@]}" > "${COMMAND_FILE}"
printf '\n' >> "${COMMAND_FILE}"

START_TIME="$(date '+%Y-%m-%d %H:%M:%S %z')"
START_EPOCH="$(date +%s)"
STATUS="FAILED"

{
  echo "[Experiment] Dataset: ${DATASET_NAME}"
  echo "[Experiment] Master: ${MASTER}"
  echo "[Experiment] Input: ${INPUT_PATH}"
  echo "[Experiment] Output: ${OUTPUT_PATH}"
  echo "[Experiment] Top-K: ${TOPK}"
  echo "[Experiment] Max referrers per dst: ${MAX_REFERRERS_PER_DST}"
  echo "[Experiment] Write intermediate: ${WRITE_INTERMEDIATE}"
  echo "[Experiment] Record dir: ${RUN_RECORD_DIR}"
  echo "[Experiment] Start time: ${START_TIME}"
} | tee "${RUN_LOG}"

if "${SPARK_CMD[@]}" 2>&1 | tee -a "${RUN_LOG}"; then
  STATUS="SUCCEEDED"
fi

END_TIME="$(date '+%Y-%m-%d %H:%M:%S %z')"
END_EPOCH="$(date +%s)"
DURATION_SECONDS="$((END_EPOCH - START_EPOCH))"

cat > "${METADATA_FILE}" <<EOF
dataset_name=${DATASET_NAME}
master=${MASTER}
deploy_mode=${DEPLOY_MODE}
topk=${TOPK}
max_referrers_per_dst=${MAX_REFERRERS_PER_DST}
write_intermediate=${WRITE_INTERMEDIATE}
status=${STATUS}
start_time=${START_TIME}
end_time=${END_TIME}
duration_seconds=${DURATION_SECONDS}
input_path=${INPUT_PATH}
output_path=${OUTPUT_PATH}
record_dir=${RUN_RECORD_DIR}
log_file=${RUN_LOG}
EOF

echo "[Experiment] Status: ${STATUS}" | tee -a "${RUN_LOG}"
echo "[Experiment] Duration (seconds): ${DURATION_SECONDS}" | tee -a "${RUN_LOG}"
echo "[Experiment] Metadata saved to: ${METADATA_FILE}" | tee -a "${RUN_LOG}"

if [[ "${STATUS}" != "SUCCEEDED" ]]; then
  exit 1
fi
