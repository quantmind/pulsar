from pulsar.apps.test import unittest
from pulsar.utils.system import json

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

    def test_json(self):
        lua = Lua()
        value = json.dumps({'bla': 1, 'foo': [2,3,4]})
        lua.set_global('test_json', value)
        result = lua.execute('cjson')

class d:
    def test_simple_bytes(self):
        lua = Lua()
        self.assertEqual(lua.execute(b'return 1+6'), 7)

    def test_simple(self):
        lua = Lua()
        self.assertEqual(lua.execute('return 1+6'), 7)

    def test_set_global(self):
        lua = Lua()
        lua.set_global('abc', 89)
        result = lua.execute('return abc')
        self.assertEqual(result, 89)

    def test_python_lib(self):
        lua = Lua()
        lua.register('pytest', Handler(), 'call')
        result = lua.execute('return type(pytest)')
        self.assertEqual(result, b'table')
        result = lua.execute('return type(pytest.call)')
        self.assertEqual(result, b'function')
        result = lua.execute('return pytest.call(1,3)')
        self.assertEqual(result, [1, 3])

    def test_json(self):
        lua = Lua()
        value = json.dumps({'bla': 1, 'foo': [2,3,4]})
        result = lua.execute('cjson')

