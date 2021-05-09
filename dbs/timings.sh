#!/usr/bin/env bash

readonly QUERY="SELECT COUNT(DISTINCT sampleId) FROM gts INNER JOIN annotations ON gts.vid = annotations.vid JOIN callsets ON gts.callsetId = callsets.callsetId WHERE annotations.genesymbol = 'CDK11B';"
parquet_prefix=../tables/pq_direct

echo "DuckDB"
time duckdb dset1.duckdb "PRAGMA memory_limit='8GB'; PRAGMA threads=8; ${QUERY}"

echo "DuckDB over parquet"
time ./duckdb <<EOF
    CREATE VIEW annotations AS SELECT * FROM parquet_scan('${parquet_prefix}_annotations.parquet');
    CREATE VIEW callsets AS SELECT * FROM parquet_scan('${parquet_prefix}_callsets.parquet');
    CREATE VIEW gts AS SELECT * FROM parquet_scan('${parquet_prefix}_gts.parquet');

    PRAGMA memory_limit='8GB'; PRAGMA threads=8;

    ${QUERY}
EOF

echo "SQLite"
time sqlite3 dset1.sqlite "${QUERY}"
