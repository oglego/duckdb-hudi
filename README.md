# duckdb-hudi

🚧 WORK IN PROGRESS 🚧

Prototype of a native, zero-JVM DuckDB extension written in Rust that allows you to scan Apache Hudi tables directly into DuckDB's high-performance vectorized storage engine. 

A DuckDB extension that scans [Apache Hudi](https://hudi.apache.org/) tables directly from SQL. It is
written in Rust on top of [hudi-rs](https://github.com/apache/hudi-rs) and
[duckdb-rs](https://github.com/duckdb/duckdb-rs), so there is no JVM or Spark involved.

By leveraging `hudi-rs` and `duckdb-rs` under the hood, this extension bypasses traditional Spark/JVM runtime dependencies entirely, making it ideal for fast, local analytical queries or embedded data lakehouse pipelines.

> **Status: early release (v0.1).** The core read path works, but several features are not
> implemented yet. Please read [Limitations](#limitations) before relying on it.

## Usage

Once published to the community repository:

```sql
INSTALL hudi FROM community;
LOAD hudi;

SELECT city, count(*) AS trips, avg(fare) AS avg_fare
FROM hudi_scan('/path/to/hudi_table')
GROUP BY city;
```

`hudi_scan(path)` takes a single VARCHAR argument: the base path or URI of the table (the directory that
contains `.hoodie/`). Relative paths are resolved against the process working directory. It reads the
**latest snapshot** of the table and returns its data columns. Hudi's own metadata columns
(`_hoodie_commit_time`, `_hoodie_record_key`, and so on) are not included.

Only the columns a query actually uses are read (projection pushdown).

### Type mapping

| Arrow type                                         | DuckDB type                   |
| -------------------------------------------------- | ----------------------------- |
| Boolean                                            | BOOLEAN                       |
| Int8 / Int16 / Int32 / Int64                       | TINYINT / SMALLINT / INTEGER / BIGINT |
| UInt8 / UInt16 / UInt32 / UInt64                   | UTINYINT / USMALLINT / UINTEGER / UBIGINT |
| Float32 / Float64                                  | FLOAT / DOUBLE                |
| Date32                                             | DATE                          |
| Timestamp (no timezone, any unit)                  | TIMESTAMP (microseconds)      |
| Timestamp (with timezone, any unit)                | TIMESTAMPTZ (microseconds)    |
| Utf8 / LargeUtf8 / Utf8View                        | VARCHAR                       |
| **Anything else** (Decimal, Binary, List, Struct, Map, Date64, Time, ...) | **VARCHAR**, rendered with Arrow's text formatting |

Note in particular that `DECIMAL` columns currently come back as VARCHAR, and nested types are not
mapped to DuckDB `LIST`/`STRUCT`/`MAP`.

## Limitations

- **Latest snapshot only.** No time travel and no incremental queries.
- **No filter or partition pruning.** `WHERE` clauses are applied by DuckDB after the data is read, so a
  query always reads every partition's files (for the projected columns).
- **No Hudi metadata columns.** `_hoodie_*` columns are not exposed.
- **Single-threaded scan.**
- **Tested only against copy-on-write tables** (Hudi table version 6). Merge-on-read tables and other
  table versions have not been tested.
- **Object storage is untested.** Non-local URIs (for example `s3://`) are passed through to hudi-rs, but
  no storage options are configurable from SQL and DuckDB secrets are not used.
- No `DECIMAL` or nested-type support (see the type mapping above).

## Building

You need Rust (the version required by hudi-rs), Python 3 with `venv`, `make`, `git`, and `curl`/`unzip`
(the Makefile downloads `protoc` if it is not already installed).

```shell
git clone --recurse-submodules https://github.com/oglego/duckdb-hudi.git
cd duckdb-hudi
make configure
make debug      # or: make release
```

The loadable extension is written to `build/debug/extension/hudi/hudi.duckdb_extension`
(`build/release/...` for release builds). To try it, start DuckDB with `-unsigned`:

```shell
duckdb -unsigned
```

```sql
LOAD './build/debug/extension/hudi/hudi.duckdb_extension';
SELECT count(*) FROM hudi_scan('fixtures/hudi_table');
```

### DuckDB version

Extensions built with the unstable C API only load into the exact DuckDB version they were built
for. The target version must be kept consistent between `TARGET_DUCKDB_VERSION` in the `Makefile`, the
`duckdb_version` in `.github/workflows/MainDistributionPipeline.yml`, and the `duckdb` crate version in
`Cargo.toml`.

## Testing

Tests are [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro) in `test/sql/`, run against the
small Hudi table in `fixtures/hudi_table`:

```shell
make test_debug     # or: make test_release
```

See [`fixtures/README.md`](fixtures/README.md) for how to generate additional fixtures.

## License

Apache License 2.0, see [LICENSE](LICENSE). This extension statically links
[hudi-rs](https://github.com/apache/hudi-rs) (Apache-2.0) and other third-party crates; their licenses
apply to their respective code.
