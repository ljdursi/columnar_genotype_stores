# Sandbox for playing with columnar DBs for (initially) WES/priorirtized WES genotypes

In the [`vcfs`](./vcfs) directory we have a couple of ways to generate synthetic VCFs:

* Using [stdpopsim](https://stdpopsim.readthedocs.io/en/latest/index.html), which generates population-wise realistic lineages but unrealistic variants
* Using gnomad or dbsnp to use real variants, and draw variants according allel frequencies (for gnomad, AF_afr AF_sas AF_amr AF_eas AF_nfe AF_fin AF_asj AF_oth)

In the [`tables`](./tables) directory we will convert the VCFs into normalized tables, serialized into csvs or parquet files

Finally in the [`dbs`](./dbs) directory we have a directory for each of the set of databases we'll be playing with 

TODOs:

* dbs: Presto
* dbs: Clickhouse
* tables: partition parquet files on vid?
* tables: refactor variants-to-parquet tables out into a separate module
