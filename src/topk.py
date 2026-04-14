from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import stable_less_than_sql


def build_directed_similarity(pair_similarity: DataFrame) -> DataFrame:
    forward = pair_similarity.select(
        F.col("site_a").alias("site_id"),
        F.col("site_b").alias("similar_site"),
        F.col("similarity"),
    )
    backward = pair_similarity.select(
        F.col("site_b").alias("site_id"),
        F.col("site_a").alias("similar_site"),
        F.col("similarity"),
    )
    return forward.unionByName(backward)


def compute_topk_recommendations(pair_similarity: DataFrame, topk: int) -> DataFrame:
    if topk <= 0:
        raise ValueError("topk must be a positive integer")

    directed_similarity = build_directed_similarity(pair_similarity)

    aggregated = directed_similarity.groupBy("site_id").agg(
        F.collect_list(
            F.struct(
                F.col("similarity").alias("similarity"),
                F.col("similar_site").alias("similar_site"),
            )
        ).alias("candidates")
    )

    tie_break_left = stable_less_than_sql("left.similar_site", "right.similar_site")
    tie_break_right = stable_less_than_sql("right.similar_site", "left.similar_site")

    sorted_candidates_expr = f"""
    slice(
      array_sort(
        candidates,
        (left, right) -> CASE
          WHEN left.similarity > right.similarity THEN -1
          WHEN left.similarity < right.similarity THEN 1
          WHEN {tie_break_left} THEN -1
          WHEN {tie_break_right} THEN 1
          ELSE 0
        END
      ),
      1,
      {int(topk)}
    )
    """

    ranked_candidates = aggregated.select(
        "site_id",
        F.expr(sorted_candidates_expr).alias("top_candidates"),
    )

    topk_result = ranked_candidates.select(
        "site_id",
        F.posexplode("top_candidates").alias("position", "candidate"),
    ).select(
        "site_id",
        F.col("candidate.similar_site").alias("similar_site"),
        F.col("candidate.similarity").alias("similarity"),
        (F.col("position") + F.lit(1)).alias("rank"),
    )

    return topk_result
