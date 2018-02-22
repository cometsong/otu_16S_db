import sys
import os
import csv

from itertools import groupby
from collections import OrderedDict

import attr

from .utils import log_it, now


log = log_it(logname='otudb.parser')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~

def is_fasta_header(line, lead_character='>'):
    """Is line a FAST[AQ] header? """
    return line[0] == lead_character


# Modified from https://drj11.wordpress.com/2010/02/22/python-getting-fasta-with-itertools-groupby/ 
def read_file_groups_with_headers(filehandle):
    """Group the file into chunks by lines matching header values"""
    for header,group in itertools.groupby(filehandle, is_fasta_header):
        if header:
            line = group.next()
            header_value = group.next()[1:].strip()
    else:
        sequence = ''.join(line.strip() for line in group)
        yield header_value, sequence


def read_fasta_file(filename):
    try:
        fh = open(filename, 'rU')
        return read_file_groups_with_headers(fh)
    except Exception as e:
        log.exception('Error reading CSV file %s, %s', self.filename, str(e))
        raise e



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Classes ~~~~~

@attr.s
class CSVParser(object):
    """Wrapper methods for 'csv' package"""
    filename: str = attr.ib()
    mode: str = attr.ib(default='r+')
    csvfh = attr.ib(init=False)
    dialect: str = attr.ib()
    delimiter: str = attr.ib(default=',')
    quotechar: str = attr.ib(default='"')

    @csvfh.default
    def get_csvfh(self):
        return self.open_file()

    @dialect.default
    def get_dialect(self):
        return self.sniff_dialect()


    def check_file_accessible(self, file_param, file_arg):
        """determine if file can be accessed.
        check writeable, then readable.
        """
        #TODO: use 'mode' param to check only readable or only writable?
        try:
            open(file_arg, 'w')
            log.debug(f'{file_arg} is writable.')
            return 'writable'
        except:
            try:
                open(file_arg, 'r')
                log.debug(f'{file_arg} is read-only.')
                return 'readable'
            except Exception as e:
                log.exception(f'There is a problem with file: {file_arg}')
                return False


    def open_file(self, filename: str = None):
        """open filename to use as csv file"""
        fname = filename if filename else self.filename
        mode = self.mode if self.mode else 'w+'
        try:
            fh = open(fname, mode, newline='')
            return fh
        except FileNotFoundError as e:
            log.info(f'file not found, opening for writing: {fname}')
            mode = 'w+'
            fh = open(fname, mode, newline='')
            return fh
        except OSError as oe:
            log.exception(f'There is a problem with file: {fname}')
            raise oe


    def load_data(self):
        """yield row dicts from csv file using DictReader"""
        log.info(f'Loading rows from {self.filename}')
        self.dialect = self.dialect if self.dialect else self.sniff_dialect()
        try:
            if self.csvfh:
                csvfh = self.csvfh
            else:
                csvfh = self.open_file()
            reader = csv.DictReader(csvfh,
                                    dialect=self.dialect,
                                    delimiter=self.delimiter,
                                    quotechar=self.quotechar)
            for row in reader:
                yield row
        except csv.Error as e:
            log.exception(f'Reading CSV file {self.filename}, line {reader.line_num!s}: {e!s}')


    def sniff_dialect(self):
        """find the line/ending type using csv.sniffer"""
        if not self.csvfh:
            csvfh = self.open_file(self.filename)
        else:
            csvfh = self.csvfh

        try:
            dialect = csv.Sniffer().sniff(csvfh.read(1024))
        except Exception as e:
            log.debug(f' Can\'t sniff the dialect in the file: {self.filename}')
            return None
        return dialect


    def write_headers(self, fieldnames: list = []):
        """write all column headers to filename"""
        if not fieldnames:
            log.exception(f'Missing fieldnames for {self.filename}')
            return False
        try:
            with csv.DictWriter(self.csvfh, fieldnames) as writer:
                log.info(f'Writing header of fieldnames to {self.filename}')
                writer.writeheader()
                return True
        except IOError as e:
            log.exception(f'Error writing headers to file {self.filename}, {e!s}')
            raise e


    def write(self, fieldnames: list = [], values: list = []):
        """write all values in csv format to outfile.
        Values is list of dicts w/ keys matching fieldnames.

        To write header to file, pass 'fieldnames' and omit `values`.
        """
        if not fieldnames:
            try:
                fieldnames = tuple(values[0].keys())
                log.info(f'Got the fieldnames from values.keys')
            except:
                log.exception(f'Missing fieldnames for {self.filename}')
                raise
        try:
            dialect = self.dialect
            if not dialect:
                dialect='unix'

            # with csv.DictWriter(self.csvfh, fieldnames, dialect=dialect, extrasaction='ignore') as writer:
            writer = csv.DictWriter(self.csvfh, fieldnames, dialect=dialect, extrasaction='ignore')
            if values:
                log.info(f'Writing csv to {self.filename}')
                try:
                    for row in values:
                        if isinstance(row, dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception(f'Error writing CSV file {self.filename}, {e!s}')
                    raise e
            else:
                log.info(f'Writing header of fieldnames to {self.filename}')
                writer.writeheader()
        except IOError as e:
            log.exception(f'Error opening or writing CSV file {self.filename}, {e!s}')
            raise e
        return True


    def values_to_list_dicts(self, values=[], keynames=[]):
        """pass list of lists of values and list of keys of desired dict
        This converts to list of dicts
        """
        log.debug('In values_to_node_dict')
        final_list = []

        key_dict = OrderedDict()
        try:
            for key in keynames:
                key_dict[key] = ''
        except Exception as e:
            log.exception(f'Error reading list of "keynames" {keynames}; {e!s}')
            raise e

        try:
            for vals in values:
                l = vals
                d = key_dict.copy()
                k = d.keys()
                for x in range(len(d)):
                    lx = l[x] if len(l) > x and l[x] is not None else ''
                    d[k[x]] = lx

                final_list.append(d)
        except Exception as e:
            log.exception(f'Error reading list of "values" {values}; {e!s}')
            raise e

        return final_list

