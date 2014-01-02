from pulsar.apps.test import unittest
from pulsar.apps.tasks import Task
from pulsar.apps.data import odm

from ..pulsards import StoreMixin


class Odm(StoreMixin):

    def mapper(cls, *models):
        mapper = odm.Mapper(cls.store)
        for model in models:
            mapper.register(model)
        return mapper

    def test_model(self):
        o = odm.Model(bla=1, foo=3)

    def __test_new_instance(self):
        models = self.mapper(Task)
        task = yield models.task.new(id='bjbhjscbhj', name='foo')
        self.assertEqual[task['id'], 'bjbhjscbhj']


class TestPulsardsODM(Odm, unittest.TestCase):
    pass
