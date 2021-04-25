dset1.duckdb: duckdb_from_parquet.sh
	./duckdb_from_parquet.sh ../tables/pq_direct $@

libparquet.so:
	curl -L -o $@.xz https://s3.amazonaws.com/cldellow/public/libparquet/libparquet.so.xz
	xz -d $@.xz

dset1.sqlite: sqlite_from_parquet.sh libparquet.so
	./duckdb_from_parquet.sh ../tables/pq_direct $@
