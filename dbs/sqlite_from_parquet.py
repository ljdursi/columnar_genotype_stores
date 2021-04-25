#!/usr/bin/env python3
"""
Read in a multi-sample vcf and generate both CSV and Parquet tables
for one schema for a genotype store.

The schema here we'll use separates variants:
(vid, chrom, pos, ref, alt)

genotypes:
(vid, callsetid, genotype)

callsets:
(callsetid, sampleid, call_type, dataset, consent)

and samples:
(sampleid, sample_type, ethnicity, birth_sex, patientid, dataset, consent)
"""
import argparse
import sqlite3
import pandas as pd
import pyarrow as pa
import sqlalchemy.types as sqltypes

def read_tables(prefix):
    return {
        tablename: pd.read_parquet(f'{prefix}_{tablename}.parquet', engine='pyarrow')
        for tablename in ["variants", "callsets", "samples", "gts", "annotations"]
    }

def pd_to_sql(tables, con):
    """
    Create sql database from dataframes
    """
    for tablename, table in tables.items():
        if not 'tablename' == 'variants':
            table.to_sql(tablename, con, index=False)
        else:
            table.to_sql(tablename, con, index=False,
                         dtypes = {
                            'vId': sqltypes.INTEGER(),
                            'chrom': sqltypes.VARCHAR(length=10),
                            'pos': sqltypes.INTEGER(),
                            'ref': sqltypes.VARCHAR(length=100),
                            'alt': sqltypes.VARCHAR(length=100)
                         })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert multi-sample VCF to tables")
    parser.add_argument("prefix", help="parquet file prefix", type=str)
    parser.add_argument("sqlite", help="sqlite filename", type=str)
    args = parser.parse_args()

    print("Reading tables into dataframes")
    tables = read_tables(args.prefix)

    print("Writing sqlite")
    with sqlite3.connect(args.sqlite) as con:
        pd_to_sql(tables, con)
