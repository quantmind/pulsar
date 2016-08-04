import unittest

import pulsar


class TestApi(unittest.TestCase):

    def test_get_proxy(self):
        self.assertRaises(ValueError, pulsar.get_proxy, 'shcbjsbcjcdcd')
        self.assertEqual(pulsar.get_proxy('shcbjsbcjcdcd', safe=True), None)

    def test_bad_concurrency(self):
        # bla concurrency does not exists
        return self.wait.assertRaises(ValueError, pulsar.spawn, kind='bla')

    def test_actor_coverage(self):
        '''test case for coverage'''
        return self.wait.assertRaises(pulsar.CommandNotFound,
                                      pulsar.send, 'arbiter',
                                      'sjdcbhjscbhjdbjsj', 'bla')
