#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_ROOT="${PROJECT_ROOT}/results"
FINAL_ROOT="${RESULTS_ROOT}/final"

latest_metadata_file() {
  find "${RESULTS_ROOT}/runs" -maxdepth 2 -type f -name metadata.txt 2>/dev/null | sort | tail -n 1
}

metadata_value() {
  local file="$1"
  local key="$2"
  grep "^${key}=" "${file}" | sed "s/^${key}=//"
}

merge_part_files() {
  local source_dir="$1"
  local destination_file="$2"

  shopt -s nullglob
  local parts=("${source_dir}"/part-*)
  shopt -u nullglob

  if [[ ${#parts[@]} -eq 0 ]]; then
    return 1
  fi

  : > "${destination_file}"
  while IFS= read -r part_file; do
    cat "${part_file}" >> "${destination_file}"
  done < <(printf '%s\n' "${parts[@]}" | sort)
}

copy_output_tree() {
  local source_path="$1"
  local destination_parent="$2"

  if [[ "${source_path}" == hdfs://* ]]; then
    hdfs dfs -get -f "${source_path}" "${destination_parent}/"
    return
  fi

  local local_source="${source_path}"
  if [[ "${local_source}" == file://* ]]; then
    local_source="${local_source#file://}"
  fi

  cp -R "${local_source}" "${destination_parent}/"
}

OUTPUT_PATH="${1:-}"
DATASET_NAME="${DATASET_NAME:-}"
RUN_ID="${RUN_ID:-}"

if [[ -z "${OUTPUT_PATH}" ]]; then
  METADATA_FILE="$(latest_metadata_file)"
  if [[ -z "${METADATA_FILE}" ]]; then
    echo "No output path provided and no metadata.txt found under results/runs." >&2
    exit 1
  fi

  OUTPUT_PATH="$(metadata_value "${METADATA_FILE}" output_path)"
  if [[ -z "${DATASET_NAME}" ]]; then
    DATASET_NAME="$(metadata_value "${METADATA_FILE}" dataset_name)"
  fi
fi

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(basename "${OUTPUT_PATH}")"
fi

if [[ -z "${DATASET_NAME}" ]]; then
  DATASET_NAME="$(basename "$(dirname "${OUTPUT_PATH}")")"
fi

LOCAL_EXPORT_DIR="${FINAL_ROOT}/${DATASET_NAME}/${RUN_ID}"
RAW_PARENT_DIR="${LOCAL_EXPORT_DIR}/raw"
RAW_RUN_DIR="${RAW_PARENT_DIR}/${RUN_ID}"
SUMMARY_FILE="${LOCAL_EXPORT_DIR}/export_summary.txt"

mkdir -p "${RAW_PARENT_DIR}"

copy_output_tree "${OUTPUT_PATH}" "${RAW_PARENT_DIR}"

if [[ ! -d "${RAW_RUN_DIR}" ]]; then
  echo "Expected downloaded/copied run directory not found: ${RAW_RUN_DIR}" >&2
  exit 1
fi

TOPK_LINE_COUNT=0
PAIR_LINE_COUNT=0
CLEANED_LINE_COUNT=0

if [[ -d "${RAW_RUN_DIR}/topk_recommendations" ]]; then
  merge_part_files "${RAW_RUN_DIR}/topk_recommendations" "${LOCAL_EXPORT_DIR}/topk_recommendations.txt"
  TOPK_LINE_COUNT="$(wc -l < "${LOCAL_EXPORT_DIR}/topk_recommendations.txt")"
fi

if [[ -d "${RAW_RUN_DIR}/pair_similarity" ]]; then
  merge_part_files "${RAW_RUN_DIR}/pair_similarity" "${LOCAL_EXPORT_DIR}/pair_similarity.txt"
  PAIR_LINE_COUNT="$(wc -l < "${LOCAL_EXPORT_DIR}/pair_similarity.txt")"
fi

if [[ -d "${RAW_RUN_DIR}/cleaned_edges" ]]; then
  merge_part_files "${RAW_RUN_DIR}/cleaned_edges" "${LOCAL_EXPORT_DIR}/cleaned_edges.txt"
  CLEANED_LINE_COUNT="$(wc -l < "${LOCAL_EXPORT_DIR}/cleaned_edges.txt")"
fi

cat > "${SUMMARY_FILE}" <<EOF
dataset_name=${DATASET_NAME}
run_id=${RUN_ID}
source_output_path=${OUTPUT_PATH}
local_export_dir=${LOCAL_EXPORT_DIR}
topk_file=${LOCAL_EXPORT_DIR}/topk_recommendations.txt
topk_line_count=${TOPK_LINE_COUNT}
pair_similarity_file=${LOCAL_EXPORT_DIR}/pair_similarity.txt
pair_similarity_line_count=${PAIR_LINE_COUNT}
cleaned_edges_file=${LOCAL_EXPORT_DIR}/cleaned_edges.txt
cleaned_edges_line_count=${CLEANED_LINE_COUNT}
exported_at=$(date '+%Y-%m-%d %H:%M:%S %z')
EOF

echo "Final result exported to: ${LOCAL_EXPORT_DIR}"
if [[ -f "${LOCAL_EXPORT_DIR}/topk_recommendations.txt" ]]; then
  echo "Merged top-k file: ${LOCAL_EXPORT_DIR}/topk_recommendations.txt"
fi
