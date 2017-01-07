import unittest


string1 = 'a'*20
string2 = 'b'*100


class TestPythonCode(unittest.TestCase):
    __benchmark__ = True
    __number__ = 100000

    def test_string_plus(self):
        assert len(string1 + string2) == 120

    def test_string_join(self):
        assert len(''.join((string1, string2))) == 120
