#!/usr/bin/env python3
"""Generate a larger Hudi test table (COW) with NULLs, several column types and an upsert commit.

UNTESTED starting point. Requires: pyspark and network access to fetch the Hudi Spark bundle.
The bundle version controls which Hudi table version is written; adjust HUDI_BUNDLE to match
what hudi-rs supports.
"""
import argparse
import json
import os, sys
import random
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

HUDI_BUNDLE = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
CITIES = ["san_francisco", "sao_paulo", "chennai"]


def make_rows(n, rng, start=0):
    base = datetime(2024, 1, 1)
    rows = []
    for i in range(start, start + n):
        rows.append(
            {
                "uuid": f"id-{i:08d}",
                "ts": 1_700_000_000_000 + i,
                "rider": None if rng.random() < 0.1 else f"rider-{rng.randint(0, 99)}",
                "fare": None if rng.random() < 0.1 else round(rng.uniform(5, 100), 2),
                "passengers": rng.randint(1, 4),
                "is_shared": rng.random() < 0.3,
                "trip_date": date(2024, 1, 1) + timedelta(days=rng.randint(0, 364)),
                "pickup_time": base + timedelta(seconds=rng.randint(0, 31_000_000)),
                "city": CITIES[i % len(CITIES)],
            }
        )
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--rows", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    spark = (
        SparkSession.builder.appName("duckdb-hudi-fixture")
        .config("spark.jars.packages", HUDI_BUNDLE)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    opts = {
        "hoodie.table.name": "fixture_table",
        "hoodie.datasource.write.recordkey.field": "uuid",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "city",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
    }

    rows = make_rows(args.rows, rng)
    spark.createDataFrame(rows).write.format("hudi").options(**opts).mode("overwrite").save(args.out)

    # Second commit: rewrite ~10% of the keys with a higher ts so the latest snapshot differs.
    updated = {}
    for r in rng.sample(rows, max(1, args.rows // 10)):
        u = dict(r)
        u["ts"] = r["ts"] + 10_000_000
        u["fare"] = None if rng.random() < 0.1 else round(rng.uniform(5, 100), 2)
        updated[u["uuid"]] = u
    spark.createDataFrame(list(updated.values())).write.format("hudi").options(**opts).mode("append").save(args.out)

    latest = {r["uuid"]: r for r in rows}
    latest.update(updated)
    final = list(latest.values())
    expected = {
        "rows": len(final),
        "null_fares": sum(1 for r in final if r["fare"] is None),
        "null_riders": sum(1 for r in final if r["rider"] is None),
        "rows_per_city": {c: sum(1 for r in final if r["city"] == c) for c in CITIES},
    }
    with open(f"{args.out}/../expected.json", "w") as f:
        json.dump(expected, f, indent=2)
    print(json.dumps(expected, indent=2))


if __name__ == "__main__":
    main()
