from pulsar.apps.data import odm
from pulsar.utils.system import json
from pulsar.apps.http import HttpClient

from .query import CouchDBMixin


def index_script(name, field):
    return "if (doc.{1}) index('{0}', doc.{1});".format(name, field)


class CouchDBLuceneSearch(CouchDBMixin, odm.SearchEngine):
    '''Search engine for CouchDB+Lucene'''
    def search(self):
        pass

    def upload(self, items):
        pass

    def create_table(self, manager):
        store = manager._read_store
        if store.dns == self.dns:
            name = manager._meta.table_name
            indexes = self._create_indexes(manager)
            if indexes:
                doc = yield store.request('get', store._database,
                                          '_design', name)
                doc['indexes'] = indexes
                response = yield store.update_document(doc)

    def _create_indexes(self, manager):
        defaults = ["function (doc) {"]
        search = {}
        for index in manager._meta.dfields.values():
            if index.repr_type == 'text':
                defaults.append(index_script('default', index.store_name))
        if len(defaults) > 1:
            defaults.append("};")
            search['default'] = {'index': '\n'.join(defaults)}
        return search

    def _init(self, headers=None, doc_url=None, **kw):
        bits = self._name.split('+')
        if len(bits) == 2:
            self._scheme = bits[0]
            self._name = bits[1]
        else:
            self._scheme = 'http'
        host = self._host
        if isinstance(host, tuple):
            host = '%s:%s' % host
        self._address = '%s://%s' % (self._scheme, host)
        self._http = HttpClient(loop=self._loop, **kw)


loc = 'pulsar.apps.data.stores.CouchDBLuceneSearch'
odm.register_searchengine('https+couchdb', loc)
odm.register_searchengine('http+couchdb', loc)
odm.register_searchengine('couchdb', loc)
