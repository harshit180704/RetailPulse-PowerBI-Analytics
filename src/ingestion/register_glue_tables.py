
import boto3
from pathlib import Path
from pyspark.sql import SparkSession

REGION = "ap-south-1"
DATABASE = "retailpulse_catalog"
BUCKET = "retailpulse-olist-data-2026-harshit"

DATASETS = [
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "payments",
    "products",
    "reviews",
    "sellers",
    "category_translation",
]


def main():
    project_root = Path(__file__).resolve().parents[2]

    spark = (
        SparkSession.builder
        .appName("RetailPulse-Glue-Table-Registration")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    session = boto3.Session(
        profile_name="retailpulse",
        region_name=REGION
    )
    glue = session.client("glue")

    try:
        for dataset in DATASETS:
            local_path = (
                project_root
                / "data"
                / "processed"
                / "silver"
                / dataset
            )

            # Read the actual Parquet schema.
            df = spark.read.parquet(str(local_path))
            schema = df.schema.jsonValue()

            columns = [
                {
                    "Name": field["name"],
                    "Type": spark_type_to_glue(field["type"])
                }
                for field in schema["fields"]
            ]

            table_name = dataset
            s3_location = f"s3://{BUCKET}/silver/{dataset}/"

            table_input = {
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                    "EXTERNAL": "TRUE"
                },
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": s3_location,
                    "InputFormat": (
                        "org.apache.hadoop.hive.ql.io.parquet."
                        "MapredParquetInputFormat"
                    ),
                    "OutputFormat": (
                        "org.apache.hadoop.hive.ql.io.parquet."
                        "MapredParquetOutputFormat"
                    ),
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.ql.io.parquet."
                            "serde.ParquetHiveSerDe"
                        ),
                        "Parameters": {
                            "serialization.format": "1"
                        }
                    }
                }
            }

            glue.create_table(
                DatabaseName=DATABASE,
                TableInput=table_input
            )

            print(f"Registered: {DATABASE}.{table_name}")
            print(f"Location: {s3_location}")
            print(f"Columns: {len(columns)}")

    finally:
        spark.stop()



def spark_type_to_glue(data_type):
    # Spark JSON schema represents primitive types as strings.
    if isinstance(data_type, str):
        primitive_types = {
            "string": "string",
            "integer": "int",
            "long": "bigint",
            "double": "double",
            "boolean": "boolean",
            "timestamp": "timestamp",
            "date": "date",
            "float": "float",
            "short": "smallint",
            "byte": "tinyint"
        }

        if data_type in primitive_types:
            return primitive_types[data_type]

        raise ValueError(f"Unsupported Spark primitive type: {data_type}")

    if isinstance(data_type, dict):
        type_name = data_type["type"]

        if type_name == "decimal":
            return (
                f"decimal({data_type['precision']},"
                f"{data_type['scale']})"
            )

        if type_name == "array":
            return f"array<{spark_type_to_glue(data_type['elementType'])}>"

        if type_name == "map":
            return (
                f"map<{spark_type_to_glue(data_type['keyType'])},"
                f"{spark_type_to_glue(data_type['valueType'])}>"
            )

        if type_name == "struct":
            fields = [
                f"`{field['name']}`:{spark_type_to_glue(field['type'])}"
                for field in data_type["fields"]
            ]
            return f"struct<{','.join(fields)}>"

    raise ValueError(f"Unsupported Spark data type: {data_type}")



if __name__ == "__main__":
    main()
