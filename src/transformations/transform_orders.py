
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "bronze"
    / "olist_orders_dataset.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "orders"
)

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("RetailPulse-Orders-Silver")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def transform_orders(spark):
    print("Reading Bronze orders dataset...")

    df = spark.read.parquet(str(INPUT_PATH))

    initial_count = df.count()
    print(f"Input records: {initial_count:,}")

    # Remove exact duplicate records
    df = df.dropDuplicates()

    # Parse timestamp columns
    for column in TIMESTAMP_COLUMNS:
        df = df.withColumn(
            column,
            F.to_timestamp(F.col(column))
        )

    # Normalize order status
    df = df.withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )

    # Validate mandatory business fields
    df = df.filter(
        F.col("order_id").isNotNull()
        & (F.trim(F.col("order_id")) != "")
        & F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
        & F.col("order_status").isNotNull()
        & (F.trim(F.col("order_status")) != "")
        & F.col("order_purchase_timestamp").isNotNull()
    )

    # Add useful derived business columns
    df = (
        df
        .withColumn(
            "purchase_date",
            F.to_date("order_purchase_timestamp")
        )
        .withColumn(
            "purchase_year",
            F.year("order_purchase_timestamp")
        )
        .withColumn(
            "purchase_month",
            F.month("order_purchase_timestamp")
        )
    )

    # Flag delivered orders with missing delivery timestamps
    df = df.withColumn(
        "delivery_timestamp_missing",
        F.when(
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNull(),
            True
        ).otherwise(False)
    )

    final_count = df.count()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

    print("\nOrders transformation completed!")
    print(f"Input records:  {initial_count:,}")
    print(f"Output records: {final_count:,}")
    print(f"Records removed: {initial_count - final_count:,}")
    print(f"Output: {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        result = transform_orders(spark)

        print("\nTransformed schema:")
        result.printSchema()

        print("\nOrder status distribution:")
        result.groupBy("order_status").count().orderBy(
            "order_status"
        ).show()

    finally:
        spark.stop()
