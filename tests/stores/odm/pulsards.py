import unittest

from pulsar.apps.tasks import Task
from pulsar.apps.data import odm

from .examples import User

from ..pulsards import StoreMixin


class Odm(StoreMixin):

    def mapper(cls, *models, **kw):
        mapper = odm.Mapper(cls.store)
        for model in models:
            mapper.register(model)
        return mapper

    def test_mapper(self):
        mapper = self.mapper()
        self.assertEqual(mapper.default_store, self.store)
        mapper.register(Task)
        self.assertEqual(mapper.task._store, self.store)
        self.assertTrue(str(mapper))

    def test_model(self):
        o = odm.Model(bla=1, foo=3)


class next_version:

    def test_insert(self):
        mapper = self.mapper(User)
        user = mapper.user
        with mapper.begin() as t:
            t.insert(user(username='foo'))
            t.insert(user(username='pippo'))
        yield t.wait()
        self.assertEqual(user['username'], 'foo')
        qs = mapper.user.filter(username='foo')
        result = yield qs.all()

    def test_new_instance(self):
        models = self.mapper(Task)
        task = yield models.task.new(id='bjbhjscbhj', name='foo')
        self.assertEqual(task['id'], 'bjbhjscbhj')


class TestPulsardsODM(Odm, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        return cls.create_pulsar_store()
