import unittest

from pulsar.apps.data import create_store


class TestMongoDbStore(unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = 'mongodb://%s' % cls.cfg.mongodb_server
        cls.store = create_store(addr)

    @classmethod
    def tearDownClass(cls):
        db = yield cls.store.database()
        result = yield db.dropDatabase()
        pass

    def test_aggregate(self):
        client = self.store.client()
        test = db.test1
        id1 = yield test.insert({"src": "Twitter", "content": "bla bla"},
                                safe=True)
        id2 = yield test.insert({"src": "Twitter", "content": "more data"},
                                safe=True)

        # Read more about the aggregation pipeline in MongoDB's docs
        pipeline = [{'$group': {'_id': '$src',
                                'content_list': {'$push': '$content'}}}]
        result = yield test.aggregate(pipeline)
        self.assertTrue(result)
