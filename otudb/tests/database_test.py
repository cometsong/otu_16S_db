import pytest

from ..database import tables

def test_columns():
    assert tables.models.otu_seq.columns == \
        ['seq_id', 'otu_name', 'sequence', 'seq_length', 'method']

