import csv
from collections import OrderedDict 

import attr

from .text import TextParser
from ..utils import log_it, now


log = log_it(logname='otudb.parser')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Classes ~~~~~

@attr.s
class CSVParser(TextParser):
    """Wrapper methods for 'csv' package"""
    dialect: str = attr.ib()
    delimiter: str = attr.ib(default=',')
    quotechar: str = attr.ib(default='"')

    @dialect.default
    def get_dialect(self):
        return self.sniff_dialect()


    def fieldnames(self):
        try:
            if self.fh.readable():
                cr = csv.DictReader(self.fh, delimiter=self.delimiter, quotechar=self.quotechar)
                self.fieldnames = cr.fieldnames
                log.info(f'fieldnames in {self.filename} retrieved.')
                return self.fieldnames
            else:
                log.info(f'fieldnames in {self.filename} not readable.')
                return []
        except Exception as e:
            log.exception(f'Reading CSV file {self.filename} cannot get fieldnames: {e!s}')
            raise e
        finally:
            self.fh.seek(0)


    def load_data(self):
        """yield row dicts from csv file using DictReader"""
        log.info(f'Loading rows from {self.filename}')
        self.dialect = self.dialect if self.dialect else self.sniff_dialect()
        try:
            reader = csv.DictReader(self.fh,
                                    dialect=self.dialect,
                                    delimiter=self.delimiter,
                                    quotechar=self.quotechar)
            for row in reader:
                yield row
        except csv.Error as e:
            log.exception(f'Reading CSV file {self.filename}, line {reader.line_num!s}: {e!s}')


    def sniff_dialect(self):
        """find the line/ending type using csv.sniffer"""
        try:
            return csv.Sniffer().sniff(self.fh.read(1024))
        except Exception as e:
            log.debug(f'Can\'t sniff the dialect in the file: {self.filename}')
            return None
        finally:
            self.fh.seek(0)


    def write(self, headers: list = [], values: list = []):
        """write all headers and values in possibly delimited
        format to outfile.

        Values is list of lists with fields in (hopefully) in same 
        order as the list of headers.

        To write only header to file, pass 'headers' and omit `values`.
        To write only values to file, pass `values`  and omit 'headers'.
        """
        try:
            dialect = self.dialect = self.dialect if self.dialect else self.sniff_dialect()
            if not dialect:
                self.dialect = dialect = 'unix'

            if headers:
                self.headers = headers
                self.write_headers(self.headers, self.delimiter)

            if values:
                writer = csv.DictWriter(self.fh, self.headers,
                                        dialect=dialect, extrasaction='ignore')
                log.info(f'Writing values to {self.filename}')
                try:
                    for row in values:
                        if isinstance(row, dict):
                            log.debug(row)
                            writer.writerow(row)
                        else:
                            log.error(f'Row of values is *not* a "dict"!! ... {row!s}')

                except Exception as e:
                    log.exception(f'Error writing CSV file {self.filename}, {e!s}')
                    raise e
        except IOError as e:
            log.exception(f'Error opening or writing CSV file {self.filename}, {e!s}')
            raise e
        return True


    def values_to_list_dicts(self, keynames=[], values=[]):
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

