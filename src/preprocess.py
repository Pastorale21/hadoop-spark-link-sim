from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw_edges(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw text lines from a local path or hdfs:/// path."""
    return spark.read.text(input_path)


def clean_edges(raw_lines: DataFrame) -> DataFrame:
    """
    Clean edge list input:
    - drop blank lines
    - drop invalid lines with field count != 2
    - trim whitespace
    - deduplicate (src_id, dst_id)
    """
    normalized_lines = raw_lines.select(F.trim(F.col("value")).alias("line"))

    tokenized = (
        normalized_lines.filter(F.col("line") != "")
        .withColumn("parts", F.split(F.col("line"), r"\s+"))
        .filter(F.size(F.col("parts")) == 2)
    )

    cleaned_edges = (
        tokenized.select(
            F.trim(F.col("parts")[0]).alias("src_id"),
            F.trim(F.col("parts")[1]).alias("dst_id"),
        )
        .filter((F.col("src_id") != "") & (F.col("dst_id") != ""))
        .dropDuplicates(["src_id", "dst_id"])
    )

    return cleaned_edges


def preprocess_edges(spark: SparkSession, input_path: str) -> DataFrame:
    return clean_edges(read_raw_edges(spark, input_path))
