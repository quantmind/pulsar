from pulsar.apps.data import odm
from pulsar.apps.http import HttpClient


class CouchDBLuceneSearch(odm.SearchEngine):
    '''Search engine for CouchDB+Lucene'''
    def search(self):
        pass

    def upload(self, items):
        pass

    def _init(self, headers=None, doc_url=None, **kw):
        bits =self._name.split('+')
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


loc = 'pulsar.apps.data.stores.CauchDBLuceneSearch'
odm.register_searchengine('https+couchdb', loc)
odm.register_searchengine('http+couchdb', loc)
odm.register_searchengine('couchdb', loc)
