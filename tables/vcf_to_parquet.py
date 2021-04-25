#!/usr/bin/env python3
"""
Read in a multi-sample vcf and generate Parquet tables 
representing a possible schema for a genotype store.

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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from cyvcf2 import VCF
import random

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

def sample_to_sampleId(samples):
    " Mapping from sample string to integer sample ids "
    return {sample: i for i, sample in enumerate(samples)}

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
        ('vId', pa.int64()),
        ('callsetId', pa.int32()),
        ('genotype', pa.string())
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

def gt_string(genotype):
    phase_sym = '/'
    if genotype[-1]:
        phase_sym = '|'
    return phase_sym.join([str(i) for i in genotype[:-1]])

def tables(vcfFile, dataset, prefix, chunksize, verbose):
    """
    Reads an opened file as a VCF and returns pandas tables
    """
    vcf_reader = VCF(vcfFile)
    samples = vcf_reader.samples

    sample_map = sample_to_sampleId(samples)
    write_callset_table(samples, dataset, prefix)
    write_sample_table(samples, dataset, prefix)

    data_tables = empty_tables()
    writers = {"variants": None, "annotations": None, "gts": None}

    for vid, record in enumerate(vcf_reader):
        data_tables["variants"]['vId'].append(vid)
        data_tables["variants"]['chrom'].append(record.CHROM)
        data_tables["variants"]['pos'].append(record.POS)
        data_tables["variants"]['ref'].append(record.REF)
        data_tables["variants"]['alt'].append(str(record.ALT[0]))
        
        if record.INFO['geneSymbol']:
            data_tables["annotations"]['vId'].append(vid)
            data_tables["annotations"]['geneSymbol'].append(record.INFO['geneSymbol'])

        # gt_types is array of 0,1,2,3==HOM_REF, HET, UNKNOWN, HOM_ALT
        has_calls = np.where(record.gt_types % 2 == 1)[0]
        for idx in has_calls:
            sid, gt = sample_map[samples[idx]], gt_string(record.genotypes[idx])
            data_tables["gts"]['vId'].append(vid)
            data_tables["gts"]['callsetId'].append(sid)
            data_tables["gts"]['genotype'].append(gt)

        if (vid+1) % chunksize == 0:
            update_files(data_tables, writers, prefix)
            data_tables = empty_tables()
            if verbose:
                print(vid+1)

    if len(data_tables["variants"]["vId"]):
        update_files(data_tables, writers, prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert multi-sample VCF to tables")
    parser.add_argument("vcffile", type=argparse.FileType(mode='rb'))
    parser.add_argument("dataset_name", help="name of dataset for this vcf", default="dataset1", type=str)
    parser.add_argument("parquet_prefix", help="parquet file prefix", type=str)
    parser.add_argument("--chunk", help="chunk size (in variants)", type=int, default=500)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    print("Reading VCF and generating dataframes...")
    tables(args.vcffile, args.dataset_name, args.parquet_prefix, args.chunk, args.verbose)
