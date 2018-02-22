import types
import json

db_fp = open('../db_auth.json', 'r')
# types.SimpleNamespace to use "obj.atrr"
_db = json.load(db_fp, object_hook=lambda d: types.SimpleNamespace(**d))

db_config = {
    'url':
        f'{_db.dbtype}{_db.user}:{_db.password}@{_db.host}:{_db.port}/{_db.database}',
    'charset': _db.charset,
}
# print(db_config['url'])
