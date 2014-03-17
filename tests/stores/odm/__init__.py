from pulsar.apps.data import odm
from pulsar.apps.tasks import Task

from ..testmodels import StoreTest, User


class Odm(StoreTest):

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
        self.assertEqual(task['_id'], 'bjbhjscbhj')
        self.assertFalse(task._modified)
        yield models.drop_tables()
