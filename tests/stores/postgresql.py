import unittest

from pulsar import Future, new_event_loop
from pulsar.apps.data import create_store
from pulsar.apps.test import check_server


OK = check_server('postgresql')

if not OK:
    try:
        import pulsar.apps.greenio.pg
    except ImportError as e:
        MSG = str(e)
    else:
        MSG = 'Requires a running postgresql database'
else:
    MSG = ''


@unittest.skipUnless(OK, MSG)
class PostgreSqlTest(object):

    @classmethod
    def setUpClass(cls):
        cls.created = []
        cls.store = create_store(cls.cfg.postgresql_server,
                                 database=cls.name('test'))
        assert cls.store.database == cls.name('test')
        return cls.createdb()

    @classmethod
    def tearDownClass(cls):
        for db in cls.created:
            yield cls.store.delete_database(db)

    @classmethod
    def createdb(cls, name=None):
        if name:
            name = cls.name(name)
        name = yield cls.store.create_database(name)
        cls.created.append(name)

    @classmethod
    def name(cls, name):
        cn = cls.__name__.lower()
        return '%s_%s_%s' % (cls.cfg.exc_id, cn, name)


class TestPostgreSqlStore(PostgreSqlTest, unittest.TestCase):

    def test_store(self):
        store = self.store
        self.assertEqual(store.name, 'postgresql')
        sql = store.sql_engine
        self.assertTrue(sql)

    def test_ping(self):
        result = self.store.ping()
        self.assertIsInstance(result, Future)
        result = yield result
        self.assertEqual(result, True)
