
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "processed" / "bronze"

DATASETS = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]


def validate_bronze():
    passed = 0
    failed = 0

    print("\nBRONZE LAYER VALIDATION")
    print("=" * 65)

    for filename in DATASETS:
        csv_path = RAW_DIR / filename
        parquet_path = BRONZE_DIR / filename.replace(".csv", ".parquet")

        try:
            source = pd.read_csv(csv_path)
            bronze = pd.read_parquet(parquet_path)

            checks = {
                "Row count": len(source) == len(bronze),
                "Column names": list(source.columns) == list(bronze.columns),
            }

            if all(checks.values()):
                print(f"PASS: {filename}")
                passed += 1
            else:
                print(f"FAIL: {filename}")
                for check, result in checks.items():
                    print(f"  {check}: {'PASS' if result else 'FAIL'}")
                failed += 1

        except Exception as error:
            print(f"ERROR: {filename} - {error}")
            failed += 1

    print("\n" + "=" * 65)
    print(f"Passed: {passed}/{len(DATASETS)}")
    print(f"Failed: {failed}/{len(DATASETS)}")

    if failed:
        raise SystemExit("Bronze validation failed.")

    print("All Bronze datasets passed validation!")


if __name__ == "__main__":
    validate_bronze()
