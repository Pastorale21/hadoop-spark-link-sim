import os
import sys
import unittest
import warnings
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from topk import compute_topk_recommendations


warnings.filterwarnings("ignore", category=ResourceWarning)


class TopKTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("linksim-test-topk")
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

    def test_compute_topk_sorts_by_similarity_then_stable_site_order(self) -> None:
        pair_similarity = self.spark.createDataFrame(
            [
                ("1", "10", 0.8),
                ("1", "2", 0.8),
                ("1", "4", 0.4),
                ("2", "3", 0.7),
            ],
            ["site_a", "site_b", "similarity"],
        )

        actual = compute_topk_recommendations(pair_similarity, topk=2).orderBy(
            "site_id", "rank", "similar_site"
        )
        expected = self.spark.createDataFrame(
            [
                ("1", "2", 0.8, 1),
                ("1", "10", 0.8, 2),
                ("2", "1", 0.8, 1),
                ("2", "3", 0.7, 2),
                ("3", "2", 0.7, 1),
                ("4", "1", 0.4, 1),
                ("10", "1", 0.8, 1),
            ],
            schema=actual.schema,
        ).orderBy("site_id", "rank", "similar_site")

        assertDataFrameEqual(actual, expected, checkRowOrder=True)


if __name__ == "__main__":
    unittest.main()
