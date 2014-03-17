from .store import CouchDBStore, CouchDbError, CouchDbNoDbError

from pulsar.utils.config import Global


class CouchDbServerTestOption(Global):
    name = 'couchdb_server'
    flags = ['--couchdb-server']
    meta = "CONNECTION_STRING"
    default = 'http+couchdb://127.0.0.1:5984'
    desc = 'Default connection string for the couchdb server'
