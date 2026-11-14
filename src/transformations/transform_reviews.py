
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "processed" / "bronze" / "olist_order_reviews_dataset.parquet"
SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver" / "reviews"


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Transform-Reviews")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(str(BRONZE_PATH))

        input_count = df.count()

        # Remove only exact duplicate records, not repeated review IDs.
        df = df.dropDuplicates()

        df = (
            df
            .withColumn("review_id", F.trim(F.col("review_id")))
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn(
                "review_comment_title",
                F.trim(F.col("review_comment_title"))
            )
            .withColumn(
                "review_comment_message",
                F.trim(F.col("review_comment_message"))
            )
            .withColumn(
                "review_creation_date",
                F.to_timestamp("review_creation_date")
            )
            .withColumn(
                "review_answer_timestamp",
                F.to_timestamp("review_answer_timestamp")
            )
            .filter(
                F.col("review_id").isNotNull()
                & (F.col("review_id") != "")
                & F.col("order_id").isNotNull()
                & (F.col("order_id") != "")
                & F.col("review_score").between(1, 5)
            )
            .withColumn(
                "has_review_title",
                F.col("review_comment_title").isNotNull()
                & (F.col("review_comment_title") != "")
            )
            .withColumn(
                "has_review_message",
                F.col("review_comment_message").isNotNull()
                & (F.col("review_comment_message") != "")
            )
        )

        output_count = df.count()

        # Validate the actual row grain: one record per review/order pair.
        duplicate_keys = (
            df.groupBy("review_id", "order_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_keys > 0:
            raise ValueError(
                f"Found {duplicate_keys} duplicate (review_id, order_id) keys"
            )

        print("REVIEWS SILVER TRANSFORMATION")
        print("=" * 55)
        print(f"Input records: {input_count}")
        print(f"Output records: {output_count}")
        print(f"Records removed: {input_count - output_count}")
        print(f"Duplicate composite keys: {duplicate_keys}")

        repeated_review_ids = (
            df.groupBy("review_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"Review IDs associated with multiple orders: {repeated_review_ids}")

        df.write.mode("overwrite").parquet(str(SILVER_PATH))

        print(f"Silver output written to: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
