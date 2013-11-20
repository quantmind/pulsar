from pulsar.apps.test import unittest

try:
    from pulsar.utils import lib
except ImportError:
    lib = None


class Handler():

    def call(self, *args):
        '''A simple function to call from lua'''
        return args


@unittest.skipUnless(lib , 'Requires cython extensions')
class TestLuaRuntime(unittest.TestCase):

    def test_python_lib(self):
        lua = lib.LuaRuntime()
        lua.register('pytest', Handler(), 'call')
