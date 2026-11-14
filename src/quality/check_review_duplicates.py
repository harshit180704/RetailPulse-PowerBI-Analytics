
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_order_reviews_dataset.parquet"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Review-Key-Diagnostics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH)).dropDuplicates()

        # Identify repeated review IDs
        duplicates = (
            df.groupBy("review_id")
            .agg(
                F.count("*").alias("record_count"),
                F.countDistinct("order_id").alias("distinct_orders"),
                F.countDistinct("review_score").alias("distinct_scores"),
                F.countDistinct("review_creation_date")
                    .alias("distinct_creation_dates"),
                F.countDistinct("review_answer_timestamp")
                    .alias("distinct_answer_timestamps")
            )
            .filter(F.col("record_count") > 1)
        )

        print("REVIEW ID DUPLICATE ANALYSIS")
        print("=" * 55)

        print("Total records:", df.count())
        print("Unique review IDs:", df.select("review_id").distinct().count())
        print("Repeated review IDs:", duplicates.count())

        print("\nDuplicate ID patterns:")
        print(
            "Multiple orders:",
            duplicates.filter(F.col("distinct_orders") > 1).count()
        )
        print(
            "Same order, multiple records:",
            duplicates.filter(F.col("distinct_orders") == 1).count()
        )

        print("\nSample duplicate IDs:")
        duplicates.orderBy(
            F.desc("record_count")
        ).show(15, truncate=False)

        print("\nSample underlying records:")
        sample_ids = (
            duplicates
            .orderBy(F.desc("record_count"))
            .limit(5)
            .select("review_id")
        )

        (
            df.join(sample_ids, "review_id", "inner")
            .orderBy("review_id", "order_id")
            .show(30, truncate=False)
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
