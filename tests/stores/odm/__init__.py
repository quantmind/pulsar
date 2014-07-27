from pulsar import Future
from pulsar.utils.system import json
from pulsar.apps.data import odm
from pulsar.apps.tasks import Task
from pulsar.apps.test import run_on_actor

from ..data.testmodels import StoreTest, User, Session


@run_on_actor
class Odm(StoreTest):

    @classmethod
    def setUpClass(cls):
        cls.store = cls.create_store()
        return cls.store.create_database()

    @classmethod
    def tearDownClass(cls):
        return cls.store.delete_database()

    def test_create_instance(self):
        models = self.mapper(Task)
        yield models.create_tables()
        task = yield models.task.create(id='bjbhjscbhj', name='foo',
                                        kwargs={'bal': 'foo'})
        self.assertEqual(task.id, 'bjbhjscbhj')
        self.assertEqual(task.kwargs, {'bal': 'foo'})
        data = task.to_json()
        self.assertEqual(len(data), 3)
        self.assertFalse(task._modified)
        # make sure it is serializable
        json.dumps(data)
        task = yield models.task.get(task.id)
        self.assertEqual(task.id, 'bjbhjscbhj')
        self.assertEqual(task.kwargs, {'bal': 'foo'})
        data = json.dumps(task.to_json())
        #
        task = yield models.task.get(task.id)
        data = json.dumps(task.to_json())
        yield models.drop_tables()


class d:
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
        data = task.to_json()
        self.assertEqual(len(data), 2)
        self.assertFalse(task._modified)
        # make sure it is serializable
        json.dumps(data)
        task = yield models.task(task.id)

        yield models.drop_tables()

    def test_foreign_key(self):
        models = self.mapper(User, Session)
        self.assertEqual(len(models.registered_models), 2)
        user = yield models.user.create(username='pippo')
        self.assertEqual(user.username, 'pippo')
        self.assertTrue(user.id)
        self.assertTrue(user._rev)
        self.assertTrue(user._mapper)
        session = yield models.session.create(user=user)
        self.assertTrue(session._rev)
        self.assertTrue(session._mapper)
        self.assertEqual(session.user_id, user.id)
        self.assertEqual(session.user, user)
        user2 = yield session.user
        self.assertEqual(user, user2)
        # Now lets reload the session
        session = yield models.session.get(session.id)
        self.assertEqual(session.user_id, user.id)
        self.assertFalse('_user' in session)
        user2 = session.user
        self.assertIsInstance(user2, Future)
        user2 = yield user2
        self.assertEqual(user, user2)
        self.assertTrue('_user' in session)

    def test_save_delete(self):
        models = self.mapper(User)
        user = User(username='pippo')
        self.assertRaises(odm.OdmError, user.save)
        user = yield models.user.create(username='pippo')
        self.assertFalse(user._modified)
        user['random'] = 'hello'
        self.assertTrue(user._modified)
        yield user.save()
        self.assertFalse(user._modified)
        yield user.delete()
        yield self.async.assertRaises(models.ModelNotFound,
                                      models.user.get, user.id)
