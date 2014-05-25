'''CouchDB_ document store::

    store = create_store('http+couchdb://localhost:5984/mydb')


Models
=========

A :class:`.Model` is stored as a document in the store database, this
means instances of different models which use the same couchdb store
are stored in exactly the same database.

To differentiate and query over different types of models, the
``Type`` field is added to the document representing a model.

Queries
===========

Search
==========

https://github.com/rnewson/couchdb-lucene

CouchDBStore
=================

.. autoclass:: CouchDBStore
   :members:
   :member-order: bysource


.. _CouchDB: http://couchdb.apache.org/
'''
from base64 import b64encode, b64decode

from pulsar import asyncio, coroutine_return, wait_complete, multi_async
from pulsar.utils.system import json
from pulsar.apps.data import Store, Command, register_store
from pulsar.utils.pep import zip

from pulsar.apps.data import odm

from .query import CauchDbQuery, CouchDbError, couch_db_error, CouchDBMixin


index_view = '''function (doc) {{
    if(doc.Type == '{0}' && doc.{1} !== undefined) emit(doc.{1}, doc._rev);
}};
'''


class CouchDBStore(CouchDBMixin, Store):
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

    # DATABASE OPERATIONS
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

    # DESIGN VIEWS
    def design_create(self, name, views, language=None, **kwargs):
        '''Create a new design document
        '''
        return self.request('put', self._database, '_design', name,
                            views=views,
                            language=language or 'javascript', **kwargs)

    @wait_complete
    def design_delete(self, name):
        '''Delete an existing design document at ``name``.
        '''
        response = yield self.request('head', self._database, '_design', name)
        if response.status_code == 200:
            rev = json.loads(response.headers['etag'])
            yield self.request('delete', self._database, '_design', name,
                               rev=rev)

    def design_info(self, name):
        return self.request('get', self._database, '_design', name, '_info')

    # DOCUMENTS
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
        if remove_existing:
            yield self.design_delete(table)
        views = {}
        for index in meta.indexes:
            name = key = index.store_name
            if index.primary_key:
                name = 'id'
                key = '_id'
            views[name] = {'map': index_view.format(table, key),
                           'reduce': '_count'}
        result = yield self.design_create(table, views)
        coroutine_return(result)

    def drop_table(self, model):
        '''Drop model table by simply removing model design views'''
        return self.design_delete(model._meta.table_name)

    def table_info(self, model):
        return self.design_info(model._meta.table_name)

    def execute_transaction(self, transaction):
        '''Execute a ``transaction``
        '''
        updates = []
        models = []
        for command in transaction.commands:
            action = command.action
            if not action:
                raise NotImplementedError
            else:
                model = command.args
                updates.append(dict(self.model_data(model, action)))
                models.append(model)
        #
        if updates:
            executed = yield self.update_documents(self._database, updates)
            errors = []
            for doc, model in zip(executed, models):
                if doc.get('ok'):
                    model['id'] = doc['id']
                    model['_rev'] = doc['rev']
                    model._modified.clear()
                elif doc.get('error'):
                    errors.append(CouchDbError(doc['error'], doc['reason']))
            if errors:
                raise errors[0]
        coroutine_return(models)

    @wait_complete
    def get_model(self, manager, pkvalue):
        try:
            data = yield self.request('get', self._database, pkvalue)
        except CouchDbError:
            raise odm.ModelNotFound(pkvalue)
        else:
            coroutine_return(self.build_model(manager, data))

    def query_model_view(self, model, view_name, key=None, keys=None,
                         group=None, limit=None, include_docs=None,
                         reduce=True, method=None):
        '''Query an existing view

        All view parameters here:

        http://wiki.apache.org/couchdb/HTTP_view_API
        '''
        meta = model._meta
        kwargs = {}
        if key:
            kwargs['key'] = json.dumps(key)
        elif keys:
            kwargs['keys'] = json.dumps(keys)
        if group:
            kwargs['group'] = 'true'
        if limit:
            kwargs['limit'] = int(limit)
        if include_docs:
            kwargs['include_docs'] = 'true'
        if not reduce:
            kwargs['reduce'] = 'false'
        return self.request(method or 'get', self._database, '_design',
                            meta.table_name, '_view', view_name, **kwargs)

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
    @wait_complete
    def request(self, method, *bits, **kwargs):
        '''Execute the HTTP request'''
        if self._password:
            try:
                lock = self._lock
                if not lock:
                    self._lock = lock = asyncio.Lock(loop=self._loop)
                    yield lock.acquire()
                    url = '%s/_session' % self._address
                    response = yield self._http.post(
                        url, data={'name': self._user,
                                   'password': self._password},
                        encode_multipart=False)
                    self.fire_event('request', response)
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
        self.fire_event('request', response)
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
        if action == Command.DELETE:
            yield _id, model.id
            yield '_deleted', True
        else:
            for key, value in meta.store_data(model, self, action):
                yield _id if key == pkname else key, value
            yield 'Type', meta.table_name
        if '_rev' in model:
            yield '_rev', model['_rev']


register_store("couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("http+couchdb", "pulsar.apps.data.stores.CouchDBStore")
register_store("https+couchdb", "pulsar.apps.data.stores.CouchDBStore")
