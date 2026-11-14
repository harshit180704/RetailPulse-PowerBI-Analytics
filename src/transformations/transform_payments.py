
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_order_payments_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "payments"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Payments-Silver")
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
            .withColumn(
                "payment_type",
                F.lower(F.trim("payment_type"))
            )
        )

        df = df.filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("payment_sequential").isNotNull()
            & F.col("payment_type").isNotNull()
            & (F.col("payment_type") != "")
        )

        df = df.withColumn(
            "invalid_payment",
            F.col("payment_value").isNull()
            | (F.col("payment_value") < 0)
            | (F.col("payment_installments") < 0)
        )

        output_count = df.count()

        duplicate_keys = (
            df.groupBy("order_id", "payment_sequential")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_keys > 0:
            raise ValueError(
                f"Found {duplicate_keys} duplicate payment keys"
            )

        df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("PAYMENTS SILVER TRANSFORMATION COMPLETE")
        print(f"Input rows: {initial_count:,}")
        print(f"Output rows: {output_count:,}")
        print(f"Rows removed: {initial_count - output_count:,}")
        print(f"Duplicate payment keys: {duplicate_keys}")
        print(
            "Invalid payment records:",
            df.filter("invalid_payment").count()
        )
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
