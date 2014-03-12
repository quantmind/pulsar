from asyncio import Lock

from pulsar import coroutine_return
from pulsar.apps.http import HttpClient
from pulsar.apps.data import Store, register_store

from .client import CouchDB


DEFAULT_HEADERS = {'Accept': 'application/json, text/plain; q=0.8',
                   'content-type': 'application/json'}


class CouchDbError(Exception):

    def __init__(self, error, reason):
        self.error = error
        super(CouchDbError, self).__init__(reason)


class CouchDbNoDbError(CouchDbError):
    pass


error_classes = {'no_db_file': CouchDbNoDbError}


def couch_db_error(error=None, reason=None, **params):
    error_class = error_classes.get(reason, CouchDbError)
    raise error_class(error, reason)


class CouchDBStore(Store):
    _lock = None

    @property
    def scheme(self):
        return self._scheme

    def client(self):
        '''Return a :class:`.CouchDB` client
        '''
        return CouchDB(self)

    def create_database(self, dbname, **kw):
        '''Create a new database ``dbname``
        '''
        return self.client().createdb(dbname)

    def delete_database(self, dbname):
        '''Delete a database ``dbname``
        '''
        return self.request('delete', dbname)

    def request(self, method, *bits, **kwargs):
        '''Execute the HTTP request'''
        if self._password:
            try:
                lock = self._lock
                if not lock:
                    self._lock = lock = Lock(loop=self._loop)
                    yield lock.acquire()
                    url = '%s/_session' % self._address
                    response = yield self._http.post(
                        url, data={'name': self._user,
                                   'password': self._password},
                        encode_multipart=False)
                    if response.status_code != 200:
                        response.raise_for_status()
                        self._lock = None
                else:
                    yield lock.acquire()
            finally:
                lock.release()
        url = '%s/%s' % (self._address, '/'.join(bits))
        response = yield self._http.request(method, url, data=kwargs,
                                            headers=self.headers)
        data = response.decode_content()
        if 'error' in data:
            raise couch_db_error(**data)
        else:
            coroutine_return(data)

    def _init(self, headers=None, **kw):
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
        self.headers = DEFAULT_HEADERS.copy()
        if headers:
            self.headers.update(headers)
        self._http = HttpClient(**kw)

    def _buildurl(self):
        return '%s+%s' % (self._scheme, super(CouchDBStore, self)._buildurl())


register_store("couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("http+couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("https+couchdb", "pulsar.apps.data.stores.CouchDBStore")
