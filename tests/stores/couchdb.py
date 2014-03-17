import unittest

from pulsar import coroutine_return, multi_async, new_event_loop
from pulsar.utils.security import random_string
from pulsar.apps.data import create_store
from pulsar.apps.test import check_server
from pulsar.apps.data.stores import CouchDbError, CouchDbNoDbError


OK = check_server('couchdb')


@unittest.skipUnless(OK, 'Requires a running CouchDB server')
class CouchDbTest(object):

    @classmethod
    def create_store(cls, pool_size=2, **kw):
        addr = '%s/%s' % (cls.cfg.couchdb_server, cls.name(cls.__name__))
        return create_store(addr, pool_size=pool_size, **kw)


class TestStoreWithDb(object):
    store = None
    databases = []

    @classmethod
    def name(cls, name):
        '''A modified database name
        '''
        return ('test_%s_%s' % (cls.cfg.exc_id, name)).lower()

    @classmethod
    def createdb(cls, name, store=None):
        '''Create a new database.

        Add the newly created database name to the list of database to remove
        once this :class:`TestCase` invokes the :meth:`tearDownClass`
        method.
        '''
        if not cls.databases:
            cls.databases = []
        name = cls.name(name)
        store = store or cls.store
        result = yield store.create_database(name)
        cls.databases.append(name)
        coroutine_return(result)

    @classmethod
    def dropdbs(cls):
        if cls.databases:
            return multi_async((cls.store.delete_database(name)
                                for name in cls.databases))

    @classmethod
    def create_store(cls, address, pool_size=2, **kw):
        return create_store(address, pool_size=pool_size, **kw)


@unittest.skipUnless(OK, 'Requires a running CouchDB server')
class TestCouchDbStore(TestStoreWithDb, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.store = cls.create_store(cls.cfg.couchdb_server)

    @classmethod
    def tearDownClass(cls):
        return cls.dropdbs()

    def test_store(self):
        store = self.store
        self.assertEqual(store.name, 'couchdb')
        self.assertEqual(store.scheme[:4], 'http')
        client = self.store.client()
        self.assertEqual(client.store, store)

    def test_admin(self):
        result = yield self.store.info()
        self.assertTrue('version' in result)
        self.assertEqual(result['couchdb'], 'Welcome')

    def test_createdb(self):
        result = yield self.createdb('a')
        self.assertTrue(result['ok'])

    def test_createdb_illegal(self):
        store = self.store
        yield self.async.assertRaises(CouchDbError,
                                      store.create_database, 'bla.foo')

    def test_delete_non_existent_db(self):
        store = self.store
        name = ('r%s' % random_string()).lower()
        yield self.async.assertRaises(CouchDbError,
                                      store.delete_database, name)

    def test_databases(self):
        client = self.store.client()
        dbs = yield client.databases()
        self.assertTrue(dbs)
        self.assertTrue('_users' in dbs)

    def test_users(self):
        client = self.store.client()
        users = yield client.users()
        self.assertTrue(users)
        #self.assertTrue('_users' in dbs)

    # DOCUMENTS
    def test_get_invalid_document(self):
        client = self.store.client()
        yield self.async.assertRaises(CouchDbNoDbError,
                                      client.get, 'bla', '234234')

    def test_create_document(self):
        client = self.store.client()
        result = yield self.createdb('test1')
        self.assertTrue(result['ok'])
        result = yield client.post(self.name('test1'),
                                   {'title': 'Hello World',
                                    'author': 'lsbardel'})
        self.assertTrue(result['ok'])
        id = result['id']
        doc = yield client.get(self.name('test1'), result['id'])
        data = doc['data']
        self.assertEqual(data['author'], 'lsbardel')
        self.assertEqual(data['title'], 'Hello World')

    def test_sync_store(self):
        loop = new_event_loop()
        store = self.create_store(self.cfg.couchdb_server, loop=loop)
        self.assertEqual(store._loop, loop)
        self.assertEqual(store._http._loop, loop)
        result = store.ping()
        self.assertTrue('version' in result)
        self.assertEqual(result['couchdb'], 'Welcome')
