
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_order_items_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "order_items"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-OrderItems-Silver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH))
        initial_count = df.count()

        df = df.dropDuplicates()

        df = (
            df
            .withColumn("order_id", F.trim("order_id"))
            .withColumn("product_id", F.trim("product_id"))
            .withColumn("seller_id", F.trim("seller_id"))
            .withColumn(
                "shipping_limit_date",
                F.to_timestamp("shipping_limit_date")
            )
        )

        # Remove records with missing mandatory identifiers
        df = df.filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("product_id").isNotNull()
            & (F.col("product_id") != "")
            & F.col("seller_id").isNotNull()
            & (F.col("seller_id") != "")
            & F.col("order_item_id").isNotNull()
        )

        # Flag invalid monetary values instead of silently deleting them
        df = df.withColumn(
            "invalid_amount",
            (F.col("price").isNull())
            | (F.col("freight_value").isNull())
            | (F.col("price") < 0)
            | (F.col("freight_value") < 0)
        )

        output_count = df.count()

        # Validate composite business key
        duplicate_keys = (
            df.groupBy("order_id", "order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_keys > 0:
            raise ValueError(
                f"Found {duplicate_keys} duplicate order-item keys"
            )

        df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("ORDER ITEMS SILVER TRANSFORMATION COMPLETE")
        print(f"Input rows: {initial_count:,}")
        print(f"Output rows: {output_count:,}")
        print(f"Rows removed: {initial_count - output_count:,}")
        print(f"Duplicate composite keys: {duplicate_keys}")
        print(
            "Invalid amount records:",
            df.filter("invalid_amount").count()
        )
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
