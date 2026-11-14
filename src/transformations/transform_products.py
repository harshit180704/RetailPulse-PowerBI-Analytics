
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_products_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "products"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Products-Silver")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH))
        initial_count = df.count()

        df = df.dropDuplicates()

        # Normalize product category
        df = df.withColumn(
            "product_category_name",
            F.coalesce(
                F.lower(F.trim(F.col("product_category_name"))),
                F.lit("unknown")
            )
        )

        # Validate product ID
        df = df.withColumn(
            "product_id",
            F.trim(F.col("product_id"))
        )

        df = df.filter(
            F.col("product_id").isNotNull()
            & (F.col("product_id") != "")
        )

        # Identify incomplete physical dimensions
        df = df.withColumn(
            "dimensions_missing",
            F.col("product_weight_g").isNull()
            | F.col("product_length_cm").isNull()
            | F.col("product_height_cm").isNull()
            | F.col("product_width_cm").isNull()
        )

        # Validate physical measurements
        invalid_measurements = (
            (F.col("product_weight_g") < 0)
            | (F.col("product_length_cm") < 0)
            | (F.col("product_height_cm") < 0)
            | (F.col("product_width_cm") < 0)
            | (F.col("product_photos_qty") < 0)
        )

        df = df.withColumn(
            "invalid_measurements",
            F.coalesce(invalid_measurements, F.lit(False))
        )

        output_count = df.count()

        duplicate_ids = (
            df.groupBy("product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_ids > 0:
            raise ValueError(
                f"Found {duplicate_ids} duplicate product IDs"
            )

        df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("PRODUCTS SILVER TRANSFORMATION COMPLETE")
        print(f"Input rows: {initial_count:,}")
        print(f"Output rows: {output_count:,}")
        print(f"Rows removed: {initial_count - output_count:,}")
        print(f"Duplicate product IDs: {duplicate_ids}")
        print(
            "Products with missing dimensions:",
            df.filter("dimensions_missing").count()
        )
        print(
            "Products with invalid measurements:",
            df.filter("invalid_measurements").count()
        )
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
