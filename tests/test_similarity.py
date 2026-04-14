import os
import sys
import unittest
import warnings
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from similarity import compute_pair_similarity


warnings.filterwarnings("ignore", category=ResourceWarning)


class SimilarityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("linksim-test-similarity")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_compute_pair_similarity_returns_expected_jaccard_scores(self) -> None:
        cleaned_edges = self.spark.createDataFrame(
            [
                ("1", "10"),
                ("1", "11"),
                ("2", "10"),
                ("2", "11"),
                ("2", "12"),
                ("3", "11"),
                ("3", "12"),
                ("4", "12"),
                ("4", "13"),
                ("5", "13"),
                ("6", "99"),
            ],
            ["src_id", "dst_id"],
        )

        actual = compute_pair_similarity(
            cleaned_edges, max_referrers_per_dst=0
        ).orderBy("site_a", "site_b")
        expected = self.spark.createDataFrame(
            [
                ("1", "2", 2.0 / 3.0),
                ("1", "3", 1.0 / 3.0),
                ("2", "3", 2.0 / 3.0),
                ("2", "4", 1.0 / 4.0),
                ("3", "4", 1.0 / 3.0),
                ("4", "5", 1.0 / 2.0),
            ],
            schema=actual.schema,
        ).orderBy("site_a", "site_b")

        assertDataFrameEqual(actual, expected, checkRowOrder=True)


if __name__ == "__main__":
    unittest.main()
