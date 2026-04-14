from typing import Iterable, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


NUMERIC_ID_PATTERN = r"^[+-]?[0-9]+$"
DECIMAL_ID_TYPE = "decimal(38, 0)"


def build_spark_session(app_name: str, master: str) -> SparkSession:
    """Create a SparkSession with the requested master."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def output_path(root: str, child: str) -> str:
    """Join local or hdfs-style output paths without breaking URI schemes."""
    return root.rstrip("/") + "/" + child.lstrip("/")


def _as_column(column_or_name: Union[Column, str]) -> Column:
    return F.col(column_or_name) if isinstance(column_or_name, str) else column_or_name


def is_numeric_id(column_or_name: Union[Column, str]) -> Column:
    return _as_column(column_or_name).rlike(NUMERIC_ID_PATTERN)


def numeric_id_value(column_or_name: Union[Column, str]) -> Column:
    return _as_column(column_or_name).cast(DECIMAL_ID_TYPE)


def stable_id_less_than(left: Union[Column, str], right: Union[Column, str]) -> Column:
    """
    Compare two site IDs with the required rule:
    numeric vs numeric -> compare numeric value
    otherwise -> compare lexicographically
    """
    left_col = _as_column(left)
    right_col = _as_column(right)

    left_is_numeric = is_numeric_id(left_col)
    right_is_numeric = is_numeric_id(right_col)
    left_numeric = numeric_id_value(left_col)
    right_numeric = numeric_id_value(right_col)

    return F.when(
        left_is_numeric & right_is_numeric,
        (left_numeric < right_numeric)
        | ((left_numeric == right_numeric) & (left_col < right_col)),
    ).otherwise(left_col < right_col)


def stable_less_than_sql(left_expr: str, right_expr: str) -> str:
    """
    SQL expression equivalent to stable_id_less_than, used in array_sort comparators.
    """
    return f"""
    CASE
      WHEN {left_expr} RLIKE '{NUMERIC_ID_PATTERN}' AND {right_expr} RLIKE '{NUMERIC_ID_PATTERN}'
        THEN (
          CAST({left_expr} AS {DECIMAL_ID_TYPE}) < CAST({right_expr} AS {DECIMAL_ID_TYPE})
          OR (
            CAST({left_expr} AS {DECIMAL_ID_TYPE}) = CAST({right_expr} AS {DECIMAL_ID_TYPE})
            AND {left_expr} < {right_expr}
          )
        )
      ELSE {left_expr} < {right_expr}
    END
    """


def stable_sort_columns(column_name: str) -> list[Column]:
    """
    Deterministic total ordering for global output sorting.
    This is used only for final materialized output order.
    """
    column = F.col(column_name)
    numeric_flag = F.when(is_numeric_id(column), F.lit(0)).otherwise(F.lit(1))
    numeric_value = F.when(is_numeric_id(column), numeric_id_value(column))
    return [numeric_flag.asc(), numeric_value.asc_nulls_last(), column.asc()]


def format_similarity(column_name: str = "similarity") -> Column:
    return F.format_string("%.10f", F.col(column_name))


def write_space_separated_text(
    dataframe: DataFrame,
    path: str,
    columns: Iterable[Union[Column, str]],
    mode: str = "overwrite",
) -> None:
    rendered_columns = []
    for column in columns:
        rendered_column = _as_column(column) if isinstance(column, str) else column
        rendered_columns.append(rendered_column.cast("string"))

    (
        dataframe.select(F.concat_ws(" ", *rendered_columns).alias("value"))
        .write.mode(mode)
        .text(path)
    )
