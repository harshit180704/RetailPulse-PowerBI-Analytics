
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "bronze"
    / "olist_geolocation_dataset.parquet"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Geolocation-Conflict-Check")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(str(INPUT_PATH))

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

        conflicts = (
            df.groupBy("zip")
            .agg(
                F.countDistinct("city").alias("city_count"),
                F.countDistinct("state").alias("state_count")
            )
            .filter(
                (F.col("city_count") > 1)
                | (F.col("state_count") > 1)
            )
        )

        print("GEOLOCATION CONFLICT PROFILE")
        print("Total ZIP prefixes:", df.select("zip").distinct().count())
        print("Conflicting ZIP prefixes:", conflicts.count())

        print("\nSample conflicts:")
        conflicts.orderBy(
            F.desc("state_count"),
            F.desc("city_count")
        ).show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
