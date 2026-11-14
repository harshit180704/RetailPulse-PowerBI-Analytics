
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_customers_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "customers"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Customers-Silver")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH))
        initial_count = df.count()

        df = df.dropDuplicates()

        df = (
            df
            .withColumn("customer_id", F.trim("customer_id"))
            .withColumn(
                "customer_unique_id",
                F.trim("customer_unique_id")
            )
            .withColumn(
                "customer_city",
                F.lower(F.trim("customer_city"))
            )
            .withColumn(
                "customer_state",
                F.upper(F.trim("customer_state"))
            )
        )

        # Preserve ZIP codes as five-character strings.
        df = df.withColumn(
            "customer_zip_code_prefix",
            F.lpad(
                F.col("customer_zip_code_prefix").cast("string"),
                5,
                "0"
            )
        )

        df = df.filter(
            F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
            & F.col("customer_unique_id").isNotNull()
            & (F.col("customer_unique_id") != "")
            & F.col("customer_zip_code_prefix").rlike("^[0-9]{5}$")
            & F.col("customer_city").isNotNull()
            & (F.col("customer_city") != "")
            & F.col("customer_state").isNotNull()
            & (F.col("customer_state") != "")
        )

        output_count = df.count()

        # Check the customer_id business key.
        duplicate_ids = (
            df.groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_ids > 0:
            raise ValueError(
                f"Found {duplicate_ids} duplicate customer IDs"
            )

        df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("CUSTOMERS SILVER TRANSFORMATION COMPLETE")
        print(f"Input rows: {initial_count:,}")
        print(f"Output rows: {output_count:,}")
        print(f"Rows removed: {initial_count - output_count:,}")
        print(f"Duplicate customer IDs: {duplicate_ids}")
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
