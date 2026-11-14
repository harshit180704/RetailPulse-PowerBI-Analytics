
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "processed" / "bronze"

for file_path in sorted(BRONZE_DIR.glob("*.parquet")):
    df = pd.read_parquet(file_path)

    print("\n" + "=" * 75)
    print(f"DATASET: {file_path.name}")
    print("=" * 75)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Duplicate rows: {df.duplicated().sum():,}")

    print("\nMissing values:")
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if missing.empty:
        print("No missing values")
    else:
        print(missing.to_string())

    print("\nColumn data types:")
    print(df.dtypes.to_string())
