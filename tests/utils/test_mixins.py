import unittest


class TestMixins(unittest.TestCase):

    def testLocal(self):
        from pulsar.utils.structures import AttributeDictionary
        from pulsar.utils.log import LocalMixin
        elem = LocalMixin()
        el = elem.local
        self.assertTrue(isinstance(el, AttributeDictionary))
        self.assertEqual(id(elem.local), id(el))
        self.assertEqual(elem.local.process, None)
        elem.local.process = True
        self.assertEqual(elem.local.process, True)
