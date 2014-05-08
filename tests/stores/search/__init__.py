import os
from random import choice

from pulsar.apps.data import odm
from pulsar.apps.test import populate

from ..testmodels import StoreTest, Blog
from ..data import basic_english_words


class SearchTest(StoreTest):

    sizes = {'tiny': (10, 10),  # documents, words per document
             'small': (20, 50),
             'normal': (50, 100),
             'big': (1000, 1000),
             'huge': (10000, 10000)}

    @classmethod
    def setUpClass(cls):
        store = cls.create_store()
        yield store.create_database()
        cls.models = odm.Mapper(store)
        cls.models.register(Blog)
        cls.models.set_search_engine(cls.create_search_engine())
        yield cls.models.create_tables()
        yield cls.populate()

    @classmethod
    def tearDownClass(cls):
        return cls.models.default_store.delete_database()

    @classmethod
    def populate(cls):
        size, nwords = cls.sizes[cls.cfg.size]
        published = populate('date', size=size)
        blog = cls.models.blog
        with cls.models.begin() as t:
            for dt in published:
                title = populate('choice', 5,
                                 choice_from=basic_english_words)
                words = populate('choice', size,
                                 choice_from=basic_english_words)
                t.add(blog(published=dt, title=' '.join(title),
                           body=' '.join(words)))
        return t.wait()

    @classmethod
    def create_search_engine(cls):
        raise NotImplementedError

    def test_engine(self):
        models = self.models
        engine = models.search_engine
