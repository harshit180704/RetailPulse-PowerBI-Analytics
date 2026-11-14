
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_sellers_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "sellers"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Sellers-Silver")
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
            .withColumn("seller_id", F.trim("seller_id"))
            .withColumn(
                "seller_city",
                F.lower(F.trim("seller_city"))
            )
            .withColumn(
                "seller_state",
                F.upper(F.trim("seller_state"))
            )
            .withColumn(
                "seller_zip_code_prefix",
                F.lpad(
                    F.col("seller_zip_code_prefix").cast("string"),
                    5,
                    "0"
                )
            )
        )

        df = df.filter(
            F.col("seller_id").isNotNull()
            & (F.col("seller_id") != "")
            & F.col("seller_zip_code_prefix").rlike("^[0-9]{5}$")
            & F.col("seller_city").isNotNull()
            & (F.col("seller_city") != "")
            & F.col("seller_state").isNotNull()
            & (F.col("seller_state") != "")
        )

        output_count = df.count()

        duplicate_ids = (
            df.groupBy("seller_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_ids > 0:
            raise ValueError(
                f"Found {duplicate_ids} duplicate seller IDs"
            )

        df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("SELLERS SILVER TRANSFORMATION COMPLETE")
        print(f"Input rows: {initial_count:,}")
        print(f"Output rows: {output_count:,}")
        print(f"Rows removed: {initial_count - output_count:,}")
        print(f"Duplicate seller IDs: {duplicate_ids}")
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
