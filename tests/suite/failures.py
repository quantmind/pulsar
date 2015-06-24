'''Tests the test suite loader'''
import unittest


class TestFailures(unittest.TestCase):

    @unittest.expectedFailure
    def test_fail(self):
        self.assertEqual(1, 0, "broken")


class TestSetupFailure(unittest.TestCase):

    @unittest.expectedFailure
    def setUp(self):
        self.assertEqual(1, 0, "broken")

    def test_ok(self):
        # Never goes in here
        pass


class TestTearDownFailure(unittest.TestCase):
    processed = 0

    @unittest.expectedFailure
    def test_fail(self):
        self.__class__.processed += 1
        self.assertEqual(1, 0, "broken")

    def test_ok(self):
        self.__class__.processed += 1

    @classmethod
    def tearDownClass(cls):
        assert cls.processed == 2, "Should have processed 2"
