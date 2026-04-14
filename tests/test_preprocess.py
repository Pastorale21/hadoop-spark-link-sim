import os
import sys
import unittest
import warnings
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from preprocess import clean_edges


warnings.filterwarnings("ignore", category=ResourceWarning)


class PreprocessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("linksim-test-preprocess")
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

    def test_clean_edges_drops_blank_invalid_and_duplicate_rows(self) -> None:
        raw_lines = self.spark.createDataFrame(
            [
                ("1 10",),
                ("1 11",),
                ("1 11",),
                ("",),
                ("   ",),
                ("2 10",),
                ("7",),
                ("8 14 extra",),
                ("  3   12  ",),
            ],
            ["value"],
        )

        actual = clean_edges(raw_lines).orderBy("src_id", "dst_id")
        expected = self.spark.createDataFrame(
            [("1", "10"), ("1", "11"), ("2", "10"), ("3", "12")],
            schema=actual.schema,
        ).orderBy("src_id", "dst_id")

        assertDataFrameEqual(actual, expected, checkRowOrder=True)


if __name__ == "__main__":
    unittest.main()
