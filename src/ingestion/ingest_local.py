
from pathlib import Path
import pandas as pd
import json
from datetime import datetime, timezone

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "processed" / "bronze"

# Expected source files
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


def ingest_datasets():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    ingestion_report = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "datasets": []
    }

    for filename in DATASETS:
        source_path = RAW_DIR / filename

        if not source_path.exists():
            raise FileNotFoundError(f"Missing source file: {filename}")

        # Read source CSV without modifying the original
        df = pd.read_csv(source_path)

        # Save raw data as Parquet in Bronze
        output_path = BRONZE_DIR / filename.replace(".csv", ".parquet")
        df.to_parquet(output_path, index=False)

        report = {
            "filename": filename,
            "rows": len(df),
            "columns": len(df.columns),
            "output": str(output_path.relative_to(PROJECT_ROOT))
        }

        ingestion_report["datasets"].append(report)

        print(
            f"INGESTED: {filename} | "
            f"Rows: {len(df):,} | "
            f"Columns: {len(df.columns)}"
        )

    report_path = BRONZE_DIR / "ingestion_report.json"

    with open(report_path, "w", encoding="utf-8") as file:
        json.dump(ingestion_report, file, indent=4)

    print("\nBronze ingestion completed successfully!")
    print(f"Output directory: {BRONZE_DIR}")


if __name__ == "__main__":
    ingest_datasets()
