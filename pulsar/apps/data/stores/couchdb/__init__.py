'''CouchDB_ document store::

    store = create_store('http+couchdb://localhost:5984/mydb')
    couch = store.client()

.. _CouchDB: http://couchdb.apache.org/
'''
from .store import CouchDBStore, CouchDbError, CouchDbNoDbError

from pulsar.utils.config import TestOption


class CouchDbServerTestOption(TestOption):
    name = 'couchdb_server'
    flags = ['--couchdb-server']
    meta = "CONNECTION_STRING"
    default = 'http+couchdb://127.0.0.1:5984/testing'
    desc = 'Connection string for the couchdb server used during testing'
