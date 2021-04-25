#!/usr/bin/env bash

readonly QUERY="SELECT Count(DISTINCT callsets.sampleid) FROM annotations INNER JOIN gts ON annotations.vid = gts.vid INNER JOIN callsets ON gts.callsetid = callsets.callsetid WHERE  annotations.genesymbol = 'CDK11B'"
readonly parquet_prefix=../tables/pq_direct

echo "DuckDB"
time duckdb dset1.duckdb "${QUERY}"

echo "DuckDB over parquet"
time ./duckdb <<EOF
    CREATE VIEW annotations AS SELECT * FROM parquet_scan('${parquet_prefix}_annotations.parquet');
    CREATE VIEW callsets AS SELECT * FROM parquet_scan('${parquet_prefix}_callsets.parquet');
    CREATE VIEW gts AS SELECT * FROM parquet_scan('${parquet_prefix}_gts.parquet');
    
    ${QUERY}
EOF

echo "SQLite"
time sqlite3 dset1.sqlite "${QUERY}"
