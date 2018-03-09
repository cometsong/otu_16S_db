"""Several types of data files will be imported into the otu db.
    This module encompasses all the techniques.
"""
import os
import attr
from clize import run, parameters

from otudb.database import otudb, tables
from otudb.parsers import CSVParser, FastaParser
from otudb.utils import log_it

log = log_it(logname='import_data')

otu_data_file_imports = {
    # otu_counts
    # This file contains the percent_abundance
    'otu_table': ['OTUId', 'sample_id', '*sample_id'], # , sample_id, sample_id, ... 

    # otu_annotation
    'otu_taxa_rdp': ['otu_name', 'domain', 'phylum', 'class', 'order', 'family', 'genus'], # otu_name header empty!

    # otu_seqs
    'otu_seq_fasta': [], # standard FASTA format

    # analysis_sets
    'analysis_sets': ['set_name', 'desc'], # TODO: this file will need to be created by end user, or parsed from others?

    # sample_info
    'sample_info': ['sample_name', 'sample_type', 'sex', 'cage', 'time', 'conditions'],
}


def sample_import(filepath):
    """import sample metadata into the db"""
    log.info('Starting to import sample metadata')
    try:
        si = CSVParser(filepath, mode='r', delimiter=',')
        sample_info = tables.models.sample_info
        with otudb.transaction():
            row_count = 0
            for row in si.load_data():
                row_count+=1
                log.info('Importing: %s',row['sample_name'])
                res = sample_info.create(sample_name=row['sample_name'],
                                         sample_type=row['sample_type'],
                                         study=row['study'],
                                         sex=row['sex'],
                                         cage=row['cage'],
                                         time=row['time'],
                                        )
        log.info('Completed importing %s rows from: %s', row_count, filepath)
    except Exception as e:
        log.error(f'Whoops while importing {filepath}.')
        raise e
    pass


def analysis_import(filepath):
    """import analysis sets into the db"""
    log.info('Starting to import analysis set info.')
    sets = CSVParser(filepath, mode='r', delimiter='\t')
    pass


def fasta_import(filepath):
    """import a fasta file into the db"""
    log.info('Starting to import FASTA')
    try:
        fp = FastaParser(filepath, mode='r')
        seq_table = tables.models.otu_seq
        row_count = 0
        # fields = [seq_table.otu_name, seq_table.sequence, seq_table.sequence]
        with otudb.transaction():
            for head, seq in fp.load_data():
                row_count+=1
                log.info('Importing: %s',head)
                res = seq_table.create(otu_name=head,
                                       sequence=seq,
                                       seq_length=len(seq)
                                      )
        log.info('Completed importing %s rows from: %s', row_count, filepath)
    except Exception as e:
        log.error(f'Whoops while importing {filepath}.')
        raise e
    # else:
        


def count_table_import(filepath):
    """import an OTU table of OTUid and Sample Name(s)
        as a matrix of percent abundance values as a 
        csv file into the db
    """
    log.info('Starting to import OTU count table')
    try:
        tp = CSVParser(filepath, mode='r', delimiter='\t')
        counts = tables.models.otu_counts
        sample_info = tables.models.sample_info
        sample_names = tp.fieldnames()[1:] # first field is OTUId
        log.info(f'{tp.filename} sample list: {sample_names}')
        with otudb.transaction():
            row_count = 0
            for row in tp.load_data():
                row_count+=1
                log.info('Importing: %s',row['OTUId'])
                sample_id = 0 #TODO implement sample_info imports for relationship
                for sample in sample_names:
                    # sample_id = sample_info.get('sample_name' == sample).sample_id
                    sample_id += 1
                    if sample_id:
                        log.info('Importing: %s of %s with %s%%',row['OTUId'],sample,row[sample])
                        res = counts.create(otu_id=row['OTUId'],
                                            sample_id=sample_id,
                                            percent_abundance=row[sample]
                                            )
                    else:
                        log.info('"%s" not found in sample_info table.',sample)
        log.info('Completed importing %s rows from: %s', row_count, filepath)
    except Exception as e:
        log.error(f'Whoops while importing {filepath}.')
        raise e


def taxa_import_rdp(filehandle):
    """import a taxa annotation file into the db"""
    log.info('Importing RDP taxonomy.')
    try:
        pass
    except Exception as e:
        log.error(f'Whoops while importing {filepath} in RDP format.')
        raise e


def taxa_import_gg(filehandle):
    """import a taxa annotation file into the db"""
    log.info('Importing GreenGenes taxonomy.')
    try:
        pass
    except Exception as e:
        log.error(f'Whoops while importing {filepath} in GG format.')
        raise e



def taxa_import(filepath):
    """import a taxa annotation file into the db"""
    log.info('Starting to import taxonomy annotations.')
    try:
        tp = CSVParser(filepath, mode='r', delimiter='\t')
        with otudb.transaction():
            for row in tp.load_data():
                log.info('Determing Taxa file source...')
                if 'k_' in row.values():
                    tp.seek(0) #reset file
                    taxa_import_gg(tp)
                else:
                    tp.seek(0) #reset file
                    taxa_import_rdp(tp)
    except Exception as e:
        log.error(f'Whoops while importing {filepath}.')
        raise e



types_of_imports = parameters.one_of(
    ('sample', "sample metadata"),
    ('analysis', "analysis sets with names, descriptions"),
    ('count', "otu count table, pct abundance per sample"),
    ('fasta', "otu seq fasta"),
    ('taxa', "otu annotations (taxonomy)"),
    )

def parse_import(*,
                 filepath:['p', str]=None,
                 filetype:['t', types_of_imports]=None,
                ):
    """Perform imports of files into OTUdb, as indicated.

    :param filepath: path to file to be imported (REQUIRED)
    :param filetype: which type of file are you importing?
        
        Possible types (-t) of files to import are:

        .    sample:   sample metadata\n
        .    analysis: analysis sets with names, descriptions\n
        .    count:    otu count table, pct abundance per sample\n
        .    fasta:    otu seq fasta\n
        .    taxa:     otu annotations (taxonomy)

    """
    if not filepath or not filetype:
        log.error('    Whoops! Path *and* type of file to be imported are required...')
        return
    elif filetype == 'sample':   return sample_import(filepath)
    elif filetype == 'analysis': return analysis_import(filepath)
    elif filetype == 'fasta':    return fasta_import(filepath)
    elif filetype == 'count':    return count_table_import(filepath)
    elif filetype == 'taxa':     return taxa_import(filepath)


run(parse_import)
