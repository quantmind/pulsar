import unittest

from pulsar import coroutine_return, multi_async
from pulsar.apps.data import create_store
from pulsar.apps.data.stores import CouchDbError


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



class TestCouchDbStore(TestStoreWithDb, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.store = create_store(cls.cfg.couchdb_server)

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
        client = self.store.client()
        result = yield client.info()
        self.assertTrue('version' in result)
        self.assertEqual(result['couchdb'], 'Welcome')

    def test_createdb(self):
        result = yield self.createdb('a')
        self.assertTrue(result['ok'])

    def test_createdb_illegal(self):
        client = self.store.client()
        yield self.async.assertRaises(CouchDbError,
                                      client.createdb, 'bla.foo')

class d:
    def test_delete_non_existent_db(self):
        client = self.default_store.client()
        name = ('r%s' % random_string()).lower()
        try:
            result = yield client.deletedb(name)
        except CouchDbError as e:
            pass
        else:
            assert False, 'CouchDbError not raised'

    def test_databases(self):
        client = self.default_store.client()
        dbs = yield client.databases()
        self.assertTrue(dbs)
        self.assertTrue('_users' in dbs)

    def test_users(self):
        client = self.default_store.client()
        users = yield client.users()
        self.assertTrue(users)
        #self.assertTrue('_users' in dbs)

    # DOCUMENTS
    def test_get_invalid_document(self):
        client = self.default_store.client()
        yield self.async.assertRaises(CouchDbNoDbError,
                                      client.get, 'bla', '234234')

    def test_create_document(self):
        client = self.default_store.client()
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

