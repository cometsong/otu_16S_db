from itertools import groupby

import attr

from .text import TextParser
from ..utils import log_it, now


log = log_it(logname='parser.fasta')


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
        f = self.fh
        for header,group in groupby(f, self.is_fasta_header):
            if header:
                header_value = group.__next__()[1:].strip()
            else:
                sequence = ''.join(line.strip() for line in group)
                yield header_value, sequence


    def load_data(self):
        try:
            return self.read_file_groups_with_headers()
        except Exception as e:
            log.exception('Error reading fasta file %s, %s', self.filename, str(e))
            raise e


    def write_header(self, header: str):
        if header[0] != self.lead_character:
            header = self.lead_character + header
        return self.write_row(header)


    def write_seqs(self, bases):
        """Write all sequence bases to filename.
        'bases' can str or list
        if str, will be split into sections of 'line_width' length
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
            seqs = [bases]

        return self.write(headers=None, values=seqs, delimiter=None)

