from pprint import pprint

import attr

from playhouse.db_url import connect as db_connect
from playhouse.dataset import DataSet
from playhouse.reflection import Introspector

from munch import Munch

from utils import log_it, now
from db_config import db_config

log = log_it(logname='otudb.database')

otudb = db_connect(db_config['url'])
# pprint(otudb.__dict__)

db_table_names = ['sample_info',
                  'otu_seq',
                  'analysis_set',
                  'sample_analysis_sets',
                  'otu_counts',
                  'otu_annotation',
                  'otu_xref',
                 ]

@attr.s(cmp=False)
class OTUTables(object):
    """set of table names as reflections of existing OTU tables in the database"""
    db = attr.ib(repr=False)
    models: Munch = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.instrospect_models()
        self.column_names_to_model()

    def instrospect_models(self):
        try:
            introspect = Introspector.from_database(self.db)
            models = introspect.generate_models()
            self.models = Munch.fromDict(models)
        except Exception as e:
            log.error(f'Whoops in introspect db.')
            raise

    def column_names_to_model(self):
        for mdl in self.models:
            self.models[mdl].columns = list(self.models[mdl]._meta.columns.keys())

tables = OTUTables(otudb)

