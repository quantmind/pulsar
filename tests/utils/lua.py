from pulsar.apps.test import unittest

try:
    from pulsar.utils.lua import Lua
except ImportError:
    Lua = None


class Handler():

    def call(self, *args):
        '''A simple function to call from lua'''
        return args


@unittest.skipUnless(Lua , 'Requires cython extensions')
class TestLuaRuntime(unittest.TestCase):

    def test_python_lib(self):
        lua = Lua()
        lua.register('pytest', Handler(), 'call')
        result = lua.execute('return type(pytest)')
        self.assertEqual(result, b'table')
        result = lua.execute('return type(pytest.call)')
        self.assertEqual(result, b'function')
        result = lua.execute('return pytest.call()')
        self.assertEqual(result, b'call')

