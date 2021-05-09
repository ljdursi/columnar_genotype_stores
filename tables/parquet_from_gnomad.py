#!/usr/bin/env python3
"""
This script generates synthetic calls based on gnomad 2.1.1 exomes
"""
import argparse
import random
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cyvcf2 import VCF
import numpy as np

def write_callset_table(samples, dataset, prefix, consent=True):
    """
    Generate the list of callsets
    """
    n = len(samples)
    callsets = {
        'callsetId': list(range(n)),
        'sampleId': list(range(n)),
        'call_type': n*["germline"],
        'dataset': n*[dataset],
        'consent': n*[consent]
    }
    callsets_schema = pa.schema([
        ('callsetId', pa.int32()),
        ('sampleId', pa.int32()),
        ('call_type', pa.string()),
        ('dataset', pa.string()),
        ('consent', pa.bool_())
    ])
    df = pd.DataFrame(callsets)
    table = pa.Table.from_pandas(df, schema=callsets_schema, preserve_index=False)
    pq.write_table(table, f'{prefix}_callsets.parquet', compression='snappy')
    return 

def write_sample_table(samples, dataset, prefix, consent=True):
    """
    Generate the list of samples
    """
    n = len(samples)
    samples = {
        'sampleId': list(range(n)),
        'sample_name': list(samples),
        'ethnicity': random.choices(["Indigenous", "Arab/West Asian",
                                   "Black", "East Asian", "South Asian",
                                   "Latin American", "White"], k=n),
        'birth_sex': random.choices(["male", "female", "other"], weights=(1000,1000,1), k=n),
        'patientId': list(samples),
        'dataset': n*[dataset],
        'consent': n*[consent]
    }
    samples_schema = pa.schema([
        ('sampleId', pa.int32()),
        ('sample_name', pa.string()),
        ('ethnicity', pa.string()),
        ('birth_sex', pa.string()),
        ('patientId', pa.string()),
        ('dataset', pa.string()),
        ('consent', pa.bool_()),
    ])
    df = pd.DataFrame(samples)
    table = pa.Table.from_pandas(df, schema=samples_schema, preserve_index=False)
    pq.write_table(table, f'{prefix}_samples.parquet', compression='snappy')
    return 

def empty_tables(): 
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

    return {"variants": variant_table, "annotations": annotations, "gts": gt_table}

def get_schemas(): 
    variant_table = pa.schema([
        ('vId', pa.int64()),
        ('chrom', pa.string()),
        ('pos', pa.int32()),
        ('ref', pa.string()),
        ('alt', pa.string())
    ])

    annotations = pa.schema([
        ('vId', pa.int64()),
        ('geneSymbol', pa.string())
    ])

    gt_table = pa.schema([
        ('vId', pa.uint64()),
        ('callsetId', pa.uint32()),
        ('genotype', pa.uint8())
    ])

    return {"variants": variant_table, "annotations": annotations, "gts": gt_table}


def update_files(tables, writers, prefix):
    schemas = get_schemas();
    for table_name in ["variants", "annotations", "gts"]:
        df = pd.DataFrame(tables[table_name])
        table = pa.Table.from_pandas(df, schema=schemas[table_name], preserve_index=False)
        if not writers[table_name]:
            writers[table_name] = pq.ParquetWriter(f"{prefix}_{table_name}.parquet", schema=schemas[table_name], compression='snappy')
        writers[table_name].write_table(table=table)


def tables(filename, nsamples, dataset, prefix, chunksize, verbose):
    """
    Reads an VCF file with allele frequencies and generates parquet files
    """
    data_tables = empty_tables()
    writers = {"variants": None, "annotations": None, "gts": None}
    count = 0
    start = time.perf_counter()

    for nrec, record in enumerate(VCF(filename)):
        if not record.INFO.get('AF'):
            continue

        q = record.INFO.get("AF")
        p = 1. - q

        gts = np.random.choice([0,1,3], nsamples, p=[p*p, 2*p*q, q*q])
        samples_present = np.where(gts > 0)[0]
        gts_present = gts[samples_present]

        n_present = len(samples_present)
        if n_present == 0:
            continue

        count += 1
        data_tables["variants"]['vId'].append(count)
        data_tables["variants"]['chrom'].append(record.CHROM)
        data_tables["variants"]['pos'].append(record.POS)
        data_tables["variants"]['ref'].append(record.REF)
        data_tables["variants"]['alt'].append(str(record.ALT[0]))
        
        data_tables["gts"]['vId'] += [count]*n_present
        data_tables["gts"]['callsetId'] += list(samples_present)
        data_tables["gts"]['genotype'] += list(gts_present)

        if len(data_tables["variants"]["vId"]) % chunksize == 0:
            update_files(data_tables, writers, prefix)
            data_tables = empty_tables()
            if verbose:
                cur = time.perf_counter()
                print(f"{count} ({nrec}): {cur-start:0.4f} sec")

    if len(data_tables["variants"]["vId"]):
        update_files(data_tables, writers, prefix)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert VCF with allele frequencies to sythetic tables")
    parser.add_argument("vcf", help="", default="vcf file name", type=str)
    parser.add_argument("dataset_name", help="name of dataset for this vcf", default="dataset1", type=str)
    parser.add_argument("parquet_prefix", help="parquet file prefix", type=str)
    parser.add_argument("nsamples", help="number of samples", type=int)
    parser.add_argument("--chunk", help="chunk size (in variants)", type=int, default=500)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    print("Reading VCF and generating dataframes...")
    samples = [f"{args.dataset_name}_{i:05}" for i in range(args.nsamples)]
    write_sample_table(samples, args.dataset_name, args.parquet_prefix, consent=True)
    write_callset_table(samples, args.dataset_name, args.parquet_prefix, consent=True)
    tables(args.vcf, args.nsamples, args.dataset_name, args.parquet_prefix, args.chunk, args.verbose)