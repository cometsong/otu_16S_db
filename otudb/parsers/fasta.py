from itertools import groupby

import attr

from .text import TextParser
from ..utils import log_it, now


log = log_it(logname='otudb.parser')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Classes ~~~~~

@attr.s
class FastaParser(TextParser):
    """Wrapper methods for 'FASTA' files"""
    lead_character: str = attr.ib(default='>')
    line_width: int = attr.ib(default='80')


    def is_fasta_header(self, line):
        """Is line a FAST[AQ] header? """
        return line[0] == self.lead_character


    def read_file_groups_with_headers(self):
        """Group the file into chunks by lines matching header values"""
        # Modified from https://drj11.wordpress.com/2010/02/22/python-getting-fasta-with-itertools-groupby/ 
        f = self._fh
        for header,group in itertools.groupby(f, self.is_fasta_header):
            if header:
                line = group.next()
                header_value = group.next()[1:].strip()
            else:
                sequence = ''.join(line.strip() for line in group)
                yield header_value, sequence


    def load_data(self):
        try:
            return self.read_file_groups_with_headers()
        except Exception as e:
            log.exception('Error reading fasta file %s, %s', self.filename, str(e))
            raise e


    def write_seq(self, bases):
        """write all sequence bases to filename
        'bases' can str or list
        """
        seqs = []
        if not bases:
            log.exception(f'Missing bases for {self.filename}')
            return False
        elif isinstance(bases, str):
            while bases:
                seqs.append(bases[0:self.line_width-1])
                bases = bases[self.line_width:]
        elif isinstance(bases, list):
            seqs = bases
        else:

        return self.write(self, headers=None, values=[seqs], delimiter=None)

