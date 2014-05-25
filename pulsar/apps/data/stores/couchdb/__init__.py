from .query import CouchDbError, CouchDbNoDbError
from .store import CouchDBStore
# from .search import CouchDBLuceneSearch

from pulsar.utils.config import Global


class CouchDbServerTestOption(Global):
    name = 'couchdb_server'
    flags = ['--couchdb-server']
    meta = "CONNECTION_STRING"
    default = 'http+couchdb://127.0.0.1:5984'
    desc = 'Default connection string for the couchdb server'
