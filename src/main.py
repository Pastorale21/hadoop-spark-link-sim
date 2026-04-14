import argparse

from pyspark import StorageLevel

from preprocess import preprocess_edges
from similarity import compute_pair_similarity
from topk import compute_topk_recommendations
from utils import (
    build_spark_session,
    format_similarity,
    normalize_data_path,
    output_path,
    stable_sort_columns,
    write_space_separated_text,
)


APP_NAME = "LinkSimWebsiteSimilarity"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Website similarity detection and Top-K recommendation with Hadoop + Spark."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input edge list path. Supports local paths and hdfs:/// paths.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output root path. Supports local paths and hdfs:/// paths.",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=10,
        help="Top-K similar sites to keep for each site.",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master, for example local[*] or yarn.",
    )
    parser.add_argument(
        "--write-intermediate",
        action="store_true",
        help="Write cleaned_edges and pair_similarity output directories.",
    )
    parser.add_argument(
        "--max-referrers-per-dst",
        type=int,
        default=0,
        help=(
            "Optional cap on referrer count per dst_id for candidate generation. "
            "0 means disabled."
        ),
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.topk <= 0:
        raise ValueError("--topk must be a positive integer")

    if args.max_referrers_per_dst < 0:
        raise ValueError("--max-referrers-per-dst must be >= 0")


def write_cleaned_edges(cleaned_edges, output_root: str) -> None:
    ordered = cleaned_edges.orderBy(
        *stable_sort_columns("src_id"),
        *stable_sort_columns("dst_id"),
    )
    write_space_separated_text(
        ordered,
        output_path(output_root, "cleaned_edges"),
        ["src_id", "dst_id"],
    )


def write_pair_similarity(pair_similarity, output_root: str) -> None:
    ordered = pair_similarity.orderBy(
        *stable_sort_columns("site_a"),
        *stable_sort_columns("site_b"),
    )
    write_space_separated_text(
        ordered,
        output_path(output_root, "pair_similarity"),
        ["site_a", "site_b", format_similarity("similarity")],
    )


def write_topk(topk_result, output_root: str) -> None:
    ordered = topk_result.orderBy(
        *stable_sort_columns("site_id"),
        "rank",
        *stable_sort_columns("similar_site"),
    )
    write_space_separated_text(
        ordered,
        output_path(output_root, "topk_recommendations"),
        ["site_id", "similar_site", format_similarity("similarity"), "rank"],
    )


def main() -> None:
    args = parse_args()
    validate_args(args)

    input_path = normalize_data_path(args.input)
    output_root = normalize_data_path(args.output)

    spark = build_spark_session(APP_NAME, args.master)

    print(f"[LinkSim] Requested Spark master: {args.master}")
    print(f"[LinkSim] Effective Spark master: {spark.sparkContext.master}")
    print(f"[LinkSim] Input path (raw): {args.input}")
    print(f"[LinkSim] Input path (resolved): {input_path}")
    print(f"[LinkSim] Output root (raw): {args.output}")
    print(f"[LinkSim] Output root (resolved): {output_root}")
    print(f"[LinkSim] Top-K: {args.topk}")
    print(f"[LinkSim] Write intermediate: {args.write_intermediate}")
    print(f"[LinkSim] Max referrers per dst: {args.max_referrers_per_dst}")

    cleaned_edges = preprocess_edges(spark, input_path).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    pair_similarity = compute_pair_similarity(
        cleaned_edges,
        max_referrers_per_dst=args.max_referrers_per_dst,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    topk_result = compute_topk_recommendations(pair_similarity, args.topk)

    if args.write_intermediate:
        write_cleaned_edges(cleaned_edges, output_root)
        write_pair_similarity(pair_similarity, output_root)

    write_topk(topk_result, output_root)

    pair_similarity.unpersist()
    cleaned_edges.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
