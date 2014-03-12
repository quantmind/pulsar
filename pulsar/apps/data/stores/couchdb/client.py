
class CouchDB(object):
    '''CouchDB_ client handle.

    This is a lightweight object which wraps a :class:`.CouchDBStore`.
    '''
    __slots__ = ('store',)

    def __init__(self, store):
        self.store = store

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.store._address)
    __str__ = __repr__

    # DOCUMENTS API

    def get(self, db, id, **kwargs):
        '''retrieve an existing document from database ``db`` by ``id``.'''
        return self.store.request('get', db, id, data=kwargs)

    def put(self, db, document):
        '''Update a document'''
        return self.store.request('get', db, id, data=kwargs)

    def post(self, db, document):
        '''Create a new ``document`` for database ``db``.'''
        return self.store.request('post', db, data=document)

    # SERVER API

    def info(self):
        return self.store.request('get')

    # DATABASE API

    def databases(self):
        '''Return a list of all databases'''
        return self.store.request('get', '_all_dbs')

    def users(self):
        '''Return a list of all users'''
        return self.store.request('get', '_users')

    def createdb(self, dbname):
        '''Create a new database ``dbname``.'''
        return self.store.request('put', dbname)

    def deletedb(self, dbname):
        '''Delete an existing database ``dbname``.'''
        return self.store.request('delete', dbname)
