import os
from random import choice

from pulsar.apps.data import odm
from pulsar.apps.test import populate

from ..testmodels import StoreTest, User


def populate_from_file(size, path):
    p = os.path
    path = p.join(p.dirname(p.dirname(p.abspath(__file__))), 'data', path)
    with open(path) as f:
        all = f.read()
    all = [a for a in all.split('\n') if a]
    return populate('choice', size, choice_from=all)


email_domains = ['gmail.com', 'yahoo.com', 'bla.com', 'foo.com']


class QueryTest(StoreTest):

    sizes = {'tiny': 3,
             'small': 7,
             'normal': 50,
             'big': 1000,
             'huge': 10000}

    @classmethod
    def setUpClass(cls):
        store = cls.create_store()
        yield store.create_database()
        cls.models = odm.Mapper(store)
        cls.models.register(User)
        yield cls.models.create_tables()
        yield cls.populate()

    @classmethod
    def tearDownClass(cls):
        return cls.models.default_store.delete_database()

    @classmethod
    def populate(cls):
        size = cls.sizes[cls.cfg.size]
        names = populate_from_file(size, 'names.txt')
        surnames = populate_from_file(size, 'family_names.txt')
        user = cls.models.user
        with cls.models.begin() as t:
            usernames = set()
            emails = set([''])
            for name, surname in zip(names, surnames):
                email = ''
                domains = iter(email_domains)
                while email in emails:
                    domain = next(domains)
                    email = ('%s.%s@%s' % (name, surname, domain)).lower()
                base = ('%s%s' % (surname[0], name)).lower()
                username = base
                count = 0
                while username in usernames:
                    count += 1
                    username = '%s%s' % (base, count)
                usernames.add(username)
                emails.add(email)
                t.add(user(first_name=name, last_name=surname, email=email,
                           username=username))
        return t.wait()

    def test_user_model(self):
        meta = self.models.user._meta
        self.assertEqual(len(meta.indexes), 8)
        indexes = [f.store_name for f in meta.indexes]
        self.assertTrue('id' in indexes)

    def test_query_all(self):
        store = self.models.user._read_store
        query = self.models.user.query()
        self.assertIsInstance(query, odm.Query)
        all = yield query.all()
        self.assertTrue(all)
        self.assertEqual(len(all), self.sizes[self.cfg.size])
        for model in all:
            self.assertEqual(model._store, store)
            self.assertTrue(model['first_name'])
            self.assertTrue(model['last_name'])
            self.assertTrue(model['email'])
            self.assertEqual(model['can_login'], True)
            self.assertEqual(model['is_active'], True)
            self.assertEqual(model['is_superuser'], False)

    def test_get(self):
        all = yield self.models.user.query().all()
        m1 = choice(all)
        i1 = yield self.models.user.get(m1.id)
        self.assertEqual(m1, i1)
        yield self.async.assertRaises(odm.ModelNotFound,
                                      self.models.user.get, 'kkkkk')

    def test_filter_username(self):
        all = yield self.models.user.query().all()
        m1 = choice(all)
        models = yield self.models.user.filter(username=m1.username).all()
        self.assertTrue(models)
        for model in models:
            self.assertEqual(model.username, m1.username)

