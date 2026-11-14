from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "product_category_name_translation.parquet"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "category_translation"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Transform-Category-Translation")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(str(BRONZE_PATH))
        input_count = df.count()

        df = (
            df
            .withColumn(
                "product_category_name",
                F.lower(F.trim(F.col("product_category_name")))
            )
            .withColumn(
                "product_category_name_english",
                F.lower(F.trim(F.col("product_category_name_english")))
            )
            .filter(
                F.col("product_category_name").isNotNull()
                & (F.col("product_category_name") != "")
                & F.col("product_category_name_english").isNotNull()
                & (F.col("product_category_name_english") != "")
            )
            .dropDuplicates(["product_category_name"])
        )

        output_count = df.count()

        duplicate_keys = (
            df.groupBy("product_category_name")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_keys > 0:
            raise ValueError(
                f"Found {duplicate_keys} duplicate category keys"
            )

        print("CATEGORY TRANSLATION SILVER TRANSFORMATION")
        print("=" * 55)
        print(f"Input records: {input_count}")
        print(f"Output records: {output_count}")
        print(f"Records removed: {input_count - output_count}")
        print(f"Duplicate category keys: {duplicate_keys}")

        df.write.mode("overwrite").parquet(str(SILVER_PATH))

        print(f"Silver output written to: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()