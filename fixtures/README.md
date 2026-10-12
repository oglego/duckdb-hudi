# Test fixtures

`hudi_table/` is a small copy-on-write Hudi table (Hudi table version 6, partitioned by `city`) with 5 rows
in a single commit. It is what `test/sql/hudi_scan.test` runs against.

That table is too small to exercise batching, NULLs, other column types, or updates across commits.
`scripts/generate_fixture.py` creates a larger table for those cases.

> **The generator script has not been run yet; treat it as a starting point.**
> It needs PySpark and a Hudi Spark bundle, and the Hudi/Spark versions below may need adjusting. The
> Hudi version determines the table version written, and hudi-rs only supports certain table versions.

```shell
pip install pyspark==3.5.*
python scripts/generate_fixture.py --out fixtures/hudi_table_large --rows 10000
```

It writes the table plus `expected.json` (row count, NULL count and per-city counts computed in Python
after applying the second commit's updates). Use those numbers as the expected values in a new
SQLLogicTest, for example:

```sql
query I
SELECT count(*) FROM hudi_scan('fixtures/hudi_table_large');
----
10000
```

Check the generated directory into git only if it is reasonably small.
