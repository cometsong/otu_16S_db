import os
import pytest

from ..parser import CSVParser


class TestCSV(object):
    """test CSVParser"""

    csv_file = 'test_tmpfile.csv'

    @pytest.fixture
    def csvp(self, tmpdir):
        # setup
        filename = os.path.join(tmpdir, self.csv_file)
        csvp = CSVParser(filename) 

        yield csvp

        # teardown
        csvp.csvfh.close()
        csvp = None


    def test_csvfh(self, csvp):
        assert os.path.exists(csvp.csvfh.name) == True


    def test_write(self, csvp):
        vals = [{'a': 1, 'b': 2},
                {'a': 3, 'b': 4}]
        assert csvp.write(values=vals)


    def test_load(self, csvp):
        """test first row loaded from csvp"""
        loaded = csvp.load_data()
        for row1 in loaded:
            assert row1 == {'a': 1, 'b': 2}
            break

