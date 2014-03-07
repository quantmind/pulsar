import unittest

from pulsar.apps.data import create_store



class TestMongoDbStore(unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = 'mongodb://127.0.0.1:27017/pulsar_testing'
        cls.store = create_store(addr)

    @classmethod
    def tearDownClass(cls):
        db = yield cls.store.database()
        result = yield db.dropDatabase()
        pass

    def test_aggregate(self):
        db = yield self.store.database()
        test = db.test1
        id1 = yield test.insert({"src":"Twitter", "content":"bla bla"},
                                   safe=True)
        id2 = yield test.insert({"src":"Twitter", "content":"more data"},
                                safe=True)

        # Read more about the aggregation pipeline in MongoDB's docs
        pipeline = [
            {'$group': {'_id':'$src', 'content_list': {'$push': '$content'} } }
            ]
        result = yield test.aggregate(pipeline)
        self.assertTrue(result)
