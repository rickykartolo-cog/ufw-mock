#!/usr/bin/env python3
"""Generate sample KYC data for the runnable pipeline example."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "examples" / "data"


def main() -> int:
    spark = (
        SparkSession.builder.appName("generate-kyc-data")
        .master("local[*]")
        .getOrCreate()
    )

    inbound_path = DATA_DIR / "inbound" / "crm" / "customers"
    inbound_path.parent.mkdir(parents=True, exist_ok=True)

    df = spark.createDataFrame(
        [
            {"customer_id": "C0001", "email": "alice@example.com", "country": "au", "income": 75000},
            {"customer_id": "C0002", "email": "bob@example.com", "country": "nz", "income": 120000},
            {"customer_id": "C0003", "email": "charlie@example.com", "country": "us", "income": 95000},
            {"customer_id": "C0004", "email": None, "country": "uk", "income": 45000},
            {"customer_id": "C0005", "email": "eve@example.com", "country": "au", "income": 200000},
        ]
    )

    df.write.mode("overwrite").parquet(str(inbound_path))
    print(f"Sample data written to {inbound_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
