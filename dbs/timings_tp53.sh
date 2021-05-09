#!/usr/bin/env bash

readonly QUERY="select count(distinct gts.callsetid) from variants join gts on variants.vId = gts.vId where variants.chrom = '17' and variants.pos > 7661779 and variants.pos < 7676594;"
parquet_prefix=../tables/gnomad_wes
dbase=gnomad_wes

echo "DuckDB"
duckdb ${dbase}.duckdb <<EOF
    PRAGMA memory_limit='16GB';
    PRAGMA threads=8;
.timer on

    ${QUERY}
EOF

echo "DuckDB - index"
duckdb ${dbase}.duckdb <<EOF
    PRAGMA memory_limit='16GB';
    PRAGMA threads=8;
    create index gts_vid_idx on gts ( vId );
.timer on
    ${QUERY}
EOF

echo "DuckDB over parquet"
./duckdb <<EOF
    CREATE VIEW variants AS SELECT * FROM parquet_scan('${parquet_prefix}_variants.parquet');
    CREATE VIEW callsets AS SELECT * FROM parquet_scan('${parquet_prefix}_callsets.parquet');
    CREATE VIEW gts AS SELECT * FROM parquet_scan('${parquet_prefix}_gts.parquet');
    PRAGMA memory_limit='16GB'; PRAGMA threads=8;
.timer on

    ${QUERY}
EOF

echo "DuckDB over parquet - index"
./duckdb <<EOF
    CREATE VIEW variants AS SELECT * FROM parquet_scan('${parquet_prefix}_variants.parquet');
    CREATE VIEW callsets AS SELECT * FROM parquet_scan('${parquet_prefix}_callsets.parquet');
    CREATE VIEW gts AS SELECT * FROM parquet_scan('${parquet_prefix}_gts.parquet');
    CREATE INDEX gts_vid_idx ON gts ( vId );

    PRAGMA memory_limit='8GB'; PRAGMA threads=8;

.timer on
    ${QUERY}
EOF

echo "SQLite"
time sqlite3 ${dbase}.sqlite <<EOF
.timer on
PRAGMA threads=8;
${QUERY}
EOF
