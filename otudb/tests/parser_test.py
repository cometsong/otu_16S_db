import os
import pytest

from ..parsers import CSVParser, FastaParser


class TestCSV(object):
    """test CSVParser"""

    csv_file = 'test_tmpfile.csv'

    @pytest.fixture
    def csvp(self, tmpdir):
        # setup
        filename = os.path.join(tmpdir, self.csv_file)
        csvp = CSVParser(filename) 

        # action!
        yield csvp

        # teardown
        csvp = None


    def test_csvfh(self, csvp):
        assert os.path.exists(csvp._fh.name) == True


    def test_write(self, csvp):
        vals = [{'a': 1, 'b': 2},
                {'a': 3, 'b': 4}]
        heads = tuple(vals[0].keys())
        assert csvp.write(headers=heads, values=vals)


    def test_load(self, csvp):
        """test first row loaded from csvp"""
        loaded = csvp.load_data()
        for row1 in loaded:
            assert row1 == {'a': 1, 'b': 2}
            break


class TestFasta(object):
    """test CSVParser"""

    filename = 'test_tmpfile.fasta'

    @pytest.fixture
    def fp(self, tmpdir):
        # setup
        filename = os.path.join(tmpdir, self.filename)
        fp = FastaParser(filename) 

        # action!
        yield fp

        # teardown
        fp = None


    def test_fh_exists(self, fp):
        assert os.path.exists(fp._fh.name) == True


    def test_write_header(self, fp):
        header = '>head'
        assert fp.write_header(header)


    def test_write_seqs(self, fp):
        base_seq = ['ACGT','TCGA']
        assert fp.write_seqs(base_seq)


    def test_load(self, fp):
        """test first row loaded from fp"""
        loaded = fp.load_data()
        for row1 in loaded:
            assert row1 == ('>head', 'ACGTTCGA')
            break


