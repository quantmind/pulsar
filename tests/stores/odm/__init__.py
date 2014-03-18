from pulsar.apps.data import odm
from pulsar.apps.tasks import Task

from ..testmodels import StoreTest, User, Session


class Odm(StoreTest):

    @classmethod
    def setUpClass(cls):
        cls.store = cls.create_store()
        return cls.store.create_database()

    @classmethod
    def tearDownClass(cls):
        return cls.store.delete_database()

    def test_foreign_key_meta(self):
        models = self.mapper(User, Session)
        self.assertEqual(len(models.registered_models), 2)
        session = Session()
        self.assertEqual(session.user, None)

    def test_mapper(self):
        mapper = self.mapper()
        self.assertEqual(mapper.default_store, self.store)
        mapper.register(Task)
        self.assertEqual(mapper.task._store, self.store)
        self.assertTrue(str(mapper))

    def test_model(self):
        o = odm.Model(bla=1, foo=3)

    def test_create_instance(self):
        models = self.mapper(Task)
        yield models.create_tables()
        task = yield models.task.create(id='bjbhjscbhj', name='foo')
        self.assertEqual(task['id'], 'bjbhjscbhj')
        self.assertEqual(len(task), 2)
        self.assertFalse(task._modified)
        yield models.drop_tables()

    def test_foreign_key(self):
        models = self.mapper(User, Session)
        self.assertEqual(len(models.registered_models), 2)
        user = yield models.user.create(username='pippo')
        self.assertEqual(user.username, 'pippo')
        self.assertTrue(user.id)
        self.assertTrue(user._store)
        self.assertTrue(user._mapper)
        session = yield models.session.create(user=user)
        self.assertTrue(session._store)
        self.assertTrue(session._mapper)
        self.assertEqual(session.user_id, user.id)
        self.assertEqual(session.user, user)
        user2 = yield session.user
        self.assertEqual(user, user2)
