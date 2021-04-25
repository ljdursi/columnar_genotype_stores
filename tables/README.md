# Tables

The script [`tables_from_vcfs.py`](tables_from_vcfs.py) generates pandas tables in memory and outputs them to sqlite, duckdb, csv, and/or parquet tables.
You can then run, e.g.

```sql
SELECT Count(DISTINCT callsets.sampleid)
FROM   annotations
       INNER JOIN gts
               ON annotations.vid = gts.vid
       INNER JOIN callsets
               ON gts.callsetid = callsets.callsetid
WHERE  annotations.genesymbol = 'CDK11B'
       AND callsets.dataset = 'dset1';
```

Generating duckdb databases from pandas tables via `to_sql` is quite slow, so a bash script [`duckdb_from_parquet.sh`](duckdb_from_parquet.sh)
allows generating the db from the parquet tables.

TODOs:

* dbs: Parquet to sqlite via pandas
* dbs: Timing script - duckdb, duckdb/parquet, sqlite
* tables: genotypes as number instead of string: see how it affects file size, times
* tables: how easy is it to add columns in parquet or duckdb?  Maybe column families help?  Try w/o a separate annotations table
* dbs: Presto
* dbs: Clickhouse
* dbs: copy to duckdb in-memory?
