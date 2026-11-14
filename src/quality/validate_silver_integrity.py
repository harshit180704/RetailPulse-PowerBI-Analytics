
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"


def load(spark, name):
    return spark.read.parquet(str(SILVER_ROOT / name))


def check_fk(child, child_col, parent, parent_col, label):
    child_keys = (
        child.select(F.col(child_col).alias("key"))
        .filter(F.col("key").isNotNull())
        .distinct()
    )

    parent_keys = (
        parent.select(F.col(parent_col).alias("key"))
        .filter(F.col("key").isNotNull())
        .distinct()
    )

    unmatched = child_keys.join(parent_keys, "key", "left_anti")
    count = unmatched.count()

    print(f"{label}: {count} unmatched keys")
    if count:
        unmatched.show(10, truncate=False)

    return count


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Silver-Integrity")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        orders = load(spark, "orders")
        customers = load(spark, "customers")
        products = load(spark, "products")
        sellers = load(spark, "sellers")
        items = load(spark, "order_items")
        payments = load(spark, "payments")
        reviews = load(spark, "reviews")
        geolocation = load(spark, "geolocation")
        categories = load(spark, "category_translation")

        print("SILVER REFERENTIAL INTEGRITY REPORT")
        print("=" * 60)

        checks = [
            (items, "order_id", orders, "order_id",
             "Order items -> Orders"),
            (items, "product_id", products, "product_id",
             "Order items -> Products"),
            (items, "seller_id", sellers, "seller_id",
             "Order items -> Sellers"),
            (payments, "order_id", orders, "order_id",
             "Payments -> Orders"),
            (reviews, "order_id", orders, "order_id",
             "Reviews -> Orders"),
            (orders, "customer_id", customers, "customer_id",
             "Orders -> Customers"),
            (products, "product_category_name",
             categories, "product_category_name",
             "Products -> Category translation"),
            (customers, "customer_zip_code_prefix",
             geolocation, "geolocation_zip_code_prefix",
             "Customers -> Geolocation"),
            (sellers, "seller_zip_code_prefix",
             geolocation, "geolocation_zip_code_prefix",
             "Sellers -> Geolocation"),
        ]

        results = []
        for child, child_col, parent, parent_col, label in checks:
            results.append(
                (label, check_fk(
                    child, child_col, parent, parent_col, label
                ))
            )

        print("\nSUMMARY")
        print("=" * 60)

        for label, count in results:
            status = "PASS" if count == 0 else "REVIEW"
            print(f"{status}: {label} ({count} unmatched)")

        print("\nValidation complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
