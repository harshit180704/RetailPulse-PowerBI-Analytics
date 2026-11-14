
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2] / "data" / "processed" / "silver"


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPulse-Silver-Exception-Analysis")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        products = spark.read.parquet(str(ROOT / "products"))
        categories = spark.read.parquet(str(ROOT / "category_translation"))
        customers = spark.read.parquet(str(ROOT / "customers"))
        sellers = spark.read.parquet(str(ROOT / "sellers"))
        geo = spark.read.parquet(str(ROOT / "geolocation"))

        print("\n1. PRODUCT CATEGORY EXCEPTIONS")
        print("=" * 55)

        missing_categories = (
            products.select("product_category_name")
            .distinct()
            .join(
                categories.select("product_category_name"),
                "product_category_name",
                "left_anti"
            )
        )

        missing_categories.show(truncate=False)

        (
            products.groupBy("product_category_name")
            .count()
            .join(missing_categories, "product_category_name")
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        print("\n2. CUSTOMER GEOLOCATION EXCEPTIONS")
        print("=" * 55)

        customer_missing = (
            customers.select("customer_zip_code_prefix")
            .distinct()
            .join(
                geo.select(
                    F.col("geolocation_zip_code_prefix")
                    .alias("customer_zip_code_prefix")
                ).distinct(),
                "customer_zip_code_prefix",
                "left_anti"
            )
        )

        print("Unmatched customer ZIP prefixes:", customer_missing.count())

        print(
            "Customer records affected:",
            customers.join(customer_missing, "customer_zip_code_prefix").count()
        )

        print("\n3. SELLER GEOLOCATION EXCEPTIONS")
        print("=" * 55)

        seller_missing = (
            sellers.select("seller_zip_code_prefix")
            .distinct()
            .join(
                geo.select(
                    F.col("geolocation_zip_code_prefix")
                    .alias("seller_zip_code_prefix")
                ).distinct(),
                "seller_zip_code_prefix",
                "left_anti"
            )
        )

        print("Unmatched seller ZIP prefixes:", seller_missing.count())

        print(
            "Seller records affected:",
            sellers.join(seller_missing, "seller_zip_code_prefix").count()
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
