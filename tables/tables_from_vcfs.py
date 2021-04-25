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
import duckdb
import pandas as pd
import sqlalchemy.types as sqltypes
import vcf
import random

def callset_table(samples, dataset, consent=True):
    """
    Generate the list of callsets
    """
    n = len(samples)
    table = {
        'callsetId': list(range(n)),
        'sampleId': list(range(n)),
        'call_type': n*["germline"],
        'dataset': n*[dataset],
        'consent': n*[consent]
    }
    return pd.DataFrame(table)

def sample_table(samples, dataset, consent=True):
    """
    Generate the list of samples
    """
    n = len(samples)
    table = {
        'sampleId': list(range(n)),
        'sample_name': list(samples),
        'call_type': n*["germline"],
        'ethnicity': random.choices(["Indigenous", "Arab/West Asian",
                                   "Black", "East Asian", "South Asian",
                                   "Latin American", "White"], k=n),
        'birth_sex': random.choices(["male", "female", "other"], weights=(1000,1000,1), k=n),
        'patientId': list(samples),
        'call_type': n*["germline"],
        'dataset': n*[dataset],
        'consent': n*[consent]
    }
    return pd.DataFrame(table)

def sample_to_sampleId(samples):
    " Mapping from sample string to integer sample ids "
    return {sample: i for i, sample in enumerate(samples)}

def tables(vcfFile, dataset):
    """
    Reads an opened file as a VCF and returns pandas tables
    """
    variant_table = {
        'vId': [],
        'chrom': [],
        'pos': [],
        'ref': [],
        'alt': [],
    }

    annotations = {
        'vId': [],
        'geneSymbol': [],
    }

    gt_table = {
        'vId': [],
        'callsetId': [],
        'genotype': []
    }

    vcf_reader = vcf.Reader(vcfFile)
    samples = vcf_reader.samples

    sample_map = sample_to_sampleId(samples)
    callsets, samples = callset_table(samples, dataset), sample_table(samples, dataset)

    for vid, record in enumerate(vcf_reader):
        variant_table['vId'].append(vid)
        variant_table['chrom'].append(record.CHROM)
        variant_table['pos'].append(record.POS)
        variant_table['ref'].append(record.REF)
        variant_table['alt'].append(str(record.ALT[0]))
        
        if record.INFO['geneSymbol']:
            annotations['vId'].append(vid)
            annotations['geneSymbol'].append(record.INFO['geneSymbol'])

        for sample in record.samples:
            if sample.gt_type:
                sid = sample_map[sample.sample]
                gt_table['vId'].append(vid)
                gt_table['callsetId'].append(sid)
                gt_table['genotype'].append(sample.gt_nums)

    return {'callsets':callsets, 'samples':samples, 
            'annotations': pd.DataFrame(annotations), 
            'variants': pd.DataFrame(variant_table),
            'gts': pd.DataFrame(gt_table)}


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

def pd_to_parquet(tables, prefix):
    """
    Create parquet tables from dataframes
    """
    for tablename, table in tables.items():
        table.to_parquet(f'{prefix}_{tablename}.parquet', index=False)

def pd_to_csv(tables, prefix):
    """
    Create csv files from dataframes
    """
    for tablename, table in tables.items():
        table.to_csv(f'{prefix}_{tablename}.csv.gz', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert multi-sample VCF to tables")
    parser.add_argument("vcffile", type=argparse.FileType(mode='rb'))
    parser.add_argument("dataset_name", help="name of dataset for this vcf", default="dataset1", type=str)
    parser.add_argument("--sqlite", help="sqlite filename", type=str)
    parser.add_argument("--duckdb", help="duckdb filename", type=str)
    parser.add_argument("--parquet", help="parquet file prefix", type=str)
    parser.add_argument("--csv", help="csv file prefix", type=str)
    args = parser.parse_args()

    print("Reading VCF and generating dataframes...")
    tables = tables(args.vcffile, args.dataset_name)

    if args.sqlite:
        print("Writing sqlite")
        with sqlite3.connect(args.sqlite) as con:
            pd_to_sql(tables, con)

    if args.duckdb:
        print("Writing duckdb")
        con = duckdb.connect(args.duckdb, read_only=False)
        pd_to_sql(tables, con)

    if args.csv:
        print("Writing csv")
        pd_to_csv(tables, args.csv)

    if args.parquet:
        print("Writing parquet")
        pd_to_parquet(tables, args.parquet)