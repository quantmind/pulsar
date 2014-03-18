'''CouchDB_ document store::

    store = create_store('http+couchdb://localhost:5984/mydb')


Models
=========

A :class:`.Model` is stored as a document in the store database, this
means instances of different models which use the same couchdb store
are stored in exactly the same database.

To differentiate and query over different types of models, the
``Type`` field is added to the document representing a model.

CouchDBStore
=================

.. autoclass:: CouchDBStore
   :members:
   :member-order: bysource


.. _CouchDB: http://couchdb.apache.org/
'''
from base64 import b64encode, b64decode
from asyncio import Lock

from pulsar import coroutine_return, task, multi_async, ImproperlyConfigured
from pulsar.utils.system import json
from pulsar.apps.http import HttpClient
from pulsar.apps.data import Store, Command, register_store
from pulsar.utils.pep import zip

from pulsar.apps.data import odm

from .query import CauchDbQuery


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


all_view = '''function (doc) {{
    if(doc.Type == '{0}') emit(null, doc);
}};
'''

all_count = '''function (doc) {{
    if(doc.Type == '{0}') emit(null, 1);
}};
'''

index_view = '''function (doc) {{
    if(doc.Type == '{0}' && doc.{1} !== undefined) emit(doc.{1}, doc);
}};
'''

class CouchDBStore(Store):
    _lock = None

    @property
    def scheme(self):
        return self._scheme

    # COUCHDB operation
    def info(self):
        '''Information about the running server
        '''
        return self.request('get')
    ping = info

    def create_design(self, name, views, language=None, **kwargs):
        '''Create a new design document
        '''
        return self.request('put', self._database, '_design', name,
                            views=views,
                            language=language or 'javascript', **kwargs)

    def delete_design(self, name):
        '''Delete an existing design document at ``name``.
        '''
        return
        return self.request('delete', self._database, '_design', name)

    def create_database(self, dbname=None, **kw):
        '''Create a new database

        :param dbname: optional database name. If not provided
            the :attr:`~Store._database` is created instead.
        '''
        return self.request('put', dbname or self._database)

    def delete_database(self, dbname=None):
        '''Delete a database ``dbname``
        '''
        return self.request('delete', dbname or self._database)

    def all_databases(self):
        '''The list of all databases
        '''
        return self.request('get', '_all_dbs')

    def update_document(self, document):
        return self.request('post', self._database, **document)

    def update_documents(self, dbname, documents, new_edits=True):
        '''Bulk update/insert of documents in a database
        '''
        return self.request('post', dbname, '_bulk_docs', docs=documents,
                            new_edits=new_edits)

    # ODM
    def create_table(self, model, remove_existing=False):
        # Build views
        meta = model._meta
        table = meta.table_name
        views = {}
        for index in meta.indexes:
            if index.primary_key:
                views['all'] = {'map': all_view.format(table)}
            else:
                name = index.store_name
                views[name] = {'map': index_view.format(table, name)}
        return self.create_design(table, views)

    def drop_table(self, model):
        '''Drop model table by simply removing model design views'''
        return self.delete_design(model._meta.table_name)

    def execute_transaction(self, transaction):
        '''Execute a ``transaction``
        '''
        # loop through models
        for tmodel in transaction.models():
            if tmodel.dirty:
                raise NotImplementedError
        updates = []
        models = []
        update_insert = set((Command.INSERT, Command.UPDATE))
        for command in transaction.commands:
            action = command.action
            if not action:
                raise NotImplementedError
            elif action in update_insert:
                model = command.args
                updates.append(dict(self.model_data(model, action)))
                models.append(model)
            else:
                raise NotImplementedError
        if updates:
            executed = yield self.update_documents(self._database, updates)
            for doc, model in zip(executed, models):
                if doc.get('ok'):
                    model['id'] = doc['id']
                    model['_rev'] = doc['rev']
                    model['_store'] = self

    @task
    def get_model(self, manager, pkvalue):
        try:
            data = yield self.request('get', self._database, pkvalue)
        except CouchDbError:
            raise odm.ModelNotFound(pkvalue)
        else:
            coroutine_return(self.build_model(manager, data))

    def query_model_view(self, model, view_name, key=None, keys=None):
        meta = model._meta
        kwargs = {}
        if key:
            kwargs['key'] = json.dumps(key)
        elif keys:
            kwargs['keys'] = json.dumps(keys)
        return self.request('get', self._database, '_design', meta.table_name,
                            '_view', view_name, **kwargs)

    def compile_query(self, query):
        return CauchDbQuery(self, query)

    def build_model(self, manager, *args, **kwargs):
        pkname = manager._meta.pkname()
        if args:
            params = args[0]
            if pkname not in params:
                params[pkname] = params.pop('_id')
        return super(CouchDBStore, self).build_model(manager, *args, **kwargs)

    # INTERNALS
    @task
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
        if response.request.method == 'HEAD':
            coroutine_return(response)
        else:
            data = response.decode_content()
            if 'error' in data:
                raise couch_db_error(**data)
            else:
                coroutine_return(data)

    def encode_bytes(self, data):
        return b64encode(data).decode(self._encoding)

    def dencode_bytes(self, data):
        return b64decode(data.encode(self._encoding))

    def model_data(self, model, action):
        '''A generator of field/value pair for the store
        '''
        meta = model._meta
        pkname = meta.pkname()
        _id = '_id'
        for key, value in meta.store_data(model, self):
            yield _id if key == pkname else key, value
        yield 'Type', meta.table_name

    def _init(self, headers=None, loop=None, namespace=None, **kw):
        if not self._database:
            self._database = namespace or 'defaultdb'
        elif namespace:
            self._database = '%s_%s' % (self._database, namespace)
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
        self._http = HttpClient(loop=self._loop, **kw)

    def _buildurl(self):
        return '%s+%s' % (self._scheme, super(CouchDBStore, self)._buildurl())


register_store("couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("http+couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("https+couchdb", "pulsar.apps.data.stores.CouchDBStore")
