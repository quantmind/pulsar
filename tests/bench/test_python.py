import unittest


string1 = 'a' * 20
string2 = 'b' * 100
A_STRING = 'this-is-a-string'


class TestPythonCode(unittest.TestCase):
    __benchmark__ = True
    __number__ = 10000

    def test_string_plus(self):
        assert len(string1 + string2) == 120

    def test_string_join(self):
        assert len(''.join((string1, string2))) == 120

    def test_string_global(self):
        assert A_STRING

    def test_string_local(self):
        assert 'this-is-a-string'
