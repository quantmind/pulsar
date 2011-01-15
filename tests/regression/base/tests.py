from pulsar import test

import pulsar as package


class TestLibrary(test.TestCase):
    
    def test_version(self):
        self.assertTrue(package.VERSION)
        self.assertTrue(package.__version__)
        self.assertEqual(package.__version__,package.get_version())
        self.assertTrue(len(package.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(package, m, None))