
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_geolocation_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "silver"
    / "geolocation"
)


def most_frequent_value(df, group_column, value_column, output_column):
    counts = (
        df.groupBy(group_column, value_column)
        .count()
    )

    window = Window.partitionBy(group_column).orderBy(
        F.desc("count"),
        F.asc(value_column)
    )

    return (
        counts
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select(
            group_column,
            F.col(value_column).alias(output_column)
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Geolocation-Silver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH))

        initial_count = df.count()

        df = (
            df
            .withColumn(
                "zip",
                F.lpad(
                    F.col("geolocation_zip_code_prefix").cast("string"),
                    5,
                    "0"
                )
            )
            .withColumn("city", F.lower(F.trim("geolocation_city")))
            .withColumn("state", F.upper(F.trim("geolocation_state")))
            .dropDuplicates()
        )

        df = df.filter(
            F.col("zip").rlike("^[0-9]{5}$")
            & F.col("geolocation_lat").between(-90, 90)
            & F.col("geolocation_lng").between(-180, 180)
            & F.col("city").isNotNull()
            & (F.col("city") != "")
            & F.col("state").isNotNull()
            & (F.col("state") != "")
        )

        cleaned_count = df.count()

        # Aggregate coordinates and count geographic variations.
        summary = (
            df.groupBy("zip")
            .agg(
                F.avg("geolocation_lat").alias("latitude"),
                F.avg("geolocation_lng").alias("longitude"),
                F.count("*").alias("source_record_count"),
                F.countDistinct("city").alias("city_count"),
                F.countDistinct("state").alias("state_count")
            )
            .withColumn(
                "geolocation_conflict",
                (F.col("city_count") > 1)
                | (F.col("state_count") > 1)
            )
        )

        # Choose the most frequent city and state.
        city_lookup = most_frequent_value(
            df, "zip", "city", "city"
        )

        state_lookup = most_frequent_value(
            df, "zip", "state", "state"
        )

        silver = (
            summary
            .join(city_lookup, "zip", "left")
            .join(state_lookup, "zip", "left")
            .withColumnRenamed("zip", "geolocation_zip_code_prefix")
        )

        duplicate_keys = (
            silver.groupBy("geolocation_zip_code_prefix")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_keys > 0:
            raise ValueError("Duplicate ZIP-prefix keys found")

        output_count = silver.count()

        silver.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print("GEOLOCATION SILVER TRANSFORMATION COMPLETE")
        print(f"Original records: {initial_count:,}")
        print(f"Cleaned records: {cleaned_count:,}")
        print(f"Aggregated ZIP prefixes: {output_count:,}")
        print(
            "Conflicting ZIP prefixes:",
            silver.filter("geolocation_conflict").count()
        )
        print(f"Duplicate ZIP keys: {duplicate_keys}")
        print(f"Output: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
