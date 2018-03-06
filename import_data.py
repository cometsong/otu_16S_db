"""Several types of data files will be imported into the otu db.
    This module encompasses all the techniques.
"""
import os
import attr
from clize import run, parameters

from otudb.database import otudb, tables
from otudb.parsers import CSVParser as TsvParser, FastaParser
from otudb.utils import log_it

log = log_it(logname='import_data')

otu_data_file_imports = {
    # otu_counts
    # This file contains the percent_abundance
    'otu_table': ['OTUId', 'sample_id', '*sample_id'], # , sample_id, sample_id, ... 
    #TODO: use tablib or list-of-lists for parsing this multi-level dataset
    #TODO: determine prior to import the set_name, and the otu_seq

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
    print('Starting to import sample metadata')
    samples = TsvParser(filepath, mode='r')
    pass


def analysis_import(filepath):
    """import analysis sets into the db"""
    print('Starting to import analysis set info.')
    sets = TsvParser(filepath, mode='r')
    pass


def fasta_import(filepath):
    """import a fasta file into the db"""
    print('Starting to import FASTA')
    try:
        fp = FastaParser(filepath, mode='r')
        seq_table = tables.models.otu_seq
        # fields = [seq_table.otu_name, seq_table.sequence, seq_table.sequence]
        with otudb.transaction():
            for head, seq in fp.load_data():
                log.info('Importing: %s',head)
                res = seq_table.create(otu_name=head,
                                       sequence=seq,
                                       seq_length=len(seq)
                                      )
    except Exception as e:
        log.error(f'Whoops while importing {filepath}.')
        raise e
    # else:
        


def count_table_import(filepath):
    """import an OTU table of OTUid and Sample Name(s)
        as a matrix of percent abundance values as a 
        csv file into the db
    """
    print('Starting to import OTU count table')
    counts = TsvParser(filepath, mode='r')
    pass


def taxa_import(filepath):
    """import a taxa annotation file into the db"""
    print('Starting to import taxonomy annotations.')
    taxa = TsvParser(filepath, mode='r')
    pass


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
        print('    Whoops! Path *and* type of file to be imported are required...')
        return
    elif filetype == 'sample':   return sample_import(filepath)
    elif filetype == 'analysis': return analysis_import(filepath)
    elif filetype == 'fasta':    return fasta_import(filepath)
    elif filetype == 'count:':   return count_table_import(filepath)
    elif filetype == 'taxa:':    return taxa_import(filepath)


if __name__ == '__main__':
    run(parse_import)
