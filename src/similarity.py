from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import stable_id_less_than


def filter_edges_by_referrer_cap(
    cleaned_edges: DataFrame, max_referrers_per_dst: Optional[int]
) -> DataFrame:
    """
    Optionally drop very high in-degree referree nodes before candidate generation.
    When the cap is disabled (None or <= 0), the original cleaned edge set is returned.
    """
    if max_referrers_per_dst is None or max_referrers_per_dst <= 0:
        return cleaned_edges

    dst_referrer_counts = cleaned_edges.groupBy("dst_id").agg(
        F.countDistinct("src_id").alias("referrer_count")
    )

    allowed_dst = dst_referrer_counts.filter(
        F.col("referrer_count") <= F.lit(max_referrers_per_dst)
    ).select("dst_id")

    return cleaned_edges.join(allowed_dst, on="dst_id", how="inner")


def compute_out_degree(cleaned_edges: DataFrame) -> DataFrame:
    return cleaned_edges.groupBy("src_id").agg(F.count("*").alias("out_degree"))


def compute_intersection_counts(candidate_edges: DataFrame) -> DataFrame:
    """
    Generate unordered unique site pairs using shared dst_id and count intersections.
    """
    left_edges = candidate_edges.alias("left")
    right_edges = candidate_edges.alias("right")

    joined = (
        left_edges.join(
            right_edges,
            F.col("left.dst_id") == F.col("right.dst_id"),
            how="inner",
        )
        .where(F.col("left.src_id") != F.col("right.src_id"))
        .where(stable_id_less_than(F.col("left.src_id"), F.col("right.src_id")))
    )

    pair_candidates = joined.select(
        F.col("left.src_id").alias("site_a"),
        F.col("right.src_id").alias("site_b"),
    )

    return pair_candidates.groupBy("site_a", "site_b").agg(
        F.count("*").alias("intersection_count")
    )


def compute_pair_similarity(
    cleaned_edges: DataFrame, max_referrers_per_dst: Optional[int]
) -> DataFrame:
    """
    Compute Jaccard similarity for candidate site pairs.
    If max_referrers_per_dst is enabled, the calculation is performed on the filtered graph.
    """
    candidate_edges = filter_edges_by_referrer_cap(
        cleaned_edges, max_referrers_per_dst=max_referrers_per_dst
    )

    intersection_counts = compute_intersection_counts(candidate_edges)
    out_degree = compute_out_degree(candidate_edges)

    left_degree = out_degree.select(
        F.col("src_id").alias("site_a"),
        F.col("out_degree").alias("out_degree_a"),
    )
    right_degree = out_degree.select(
        F.col("src_id").alias("site_b"),
        F.col("out_degree").alias("out_degree_b"),
    )

    pair_similarity = (
        intersection_counts.join(left_degree, on="site_a", how="inner")
        .join(right_degree, on="site_b", how="inner")
        .withColumn(
            "union_count",
            F.col("out_degree_a") + F.col("out_degree_b") - F.col("intersection_count"),
        )
        .withColumn(
            "similarity",
            F.when(
                F.col("union_count") > 0,
                F.col("intersection_count") / F.col("union_count"),
            ).otherwise(F.lit(0.0)),
        )
        .select("site_a", "site_b", "similarity")
    )

    return pair_similarity
