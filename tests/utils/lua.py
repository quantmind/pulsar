from pulsar.apps.test import unittest
from pulsar.utils.system import json

try:
    from pulsar.utils.lua import Lua, LuaError
except ImportError:
    Lua = None


class Handler():

    def call(self, *args):
        '''A simple function to call from lua'''
        return args


pkginfo = lambda name: ('local a = {}\n'
                        'local i = 0\n'
                        'for name, _ in pairs(%s) do\n'
                        '    i = i + 1\n'
                        '    a[i] = name\n'
                        'end\n'
                        'return a') % name

@unittest.skipUnless(Lua , 'Requires cython extensions')
class TestLuaRuntime(unittest.TestCase):

    def test_load_base(self):
        lua = Lua(load_libs=False)
        self.assertRaises(LuaError, lua.execute, 'return type(pairs)')
        lua.openlibs()
        self.assertEqual(lua.execute('return type(pairs)'), b'function')

class f:
    def test_loda_base(self):
        lua = Lua(load_libs=False)
        self.assertRaises(LuaError, lua.execute, 'return type(pairs)')
        self.assertTrue(lua.loadlib(''))
        self.assertEqual(lua.execute('return type(pairs)'), b'function')
        self.assertTrue(lua.loadlib('table'))
        names = lua.execute(pkginfo('table'))
        self.assertTrue(names)

    def test_load_table(self):
        lua = Lua(load_libs=False)
        scr = 'a={}\ntable.insert(a, 3)\nreturn a'
        self.assertRaises(LuaError, lua.execute, scr)
        self.assertTrue('table' in lua.libs())
        self.assertTrue(lua.loadlib(''))
        self.assertTrue(lua.loadlib('string'))
        self.assertTrue(lua.loadlib('table'))
        self.assertEqual(lua.execute(scr), [3])


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

    def test_loadlib(self):
        lua = Lua(load_libs=False)
        scr = 'a={}\ntable.insert(a, 3)\nreturn a'
        self.assertRaises(LuaError, lua.execute, scr)
        self.assertTrue('table' in lua.libs())
        self.assertTrue(lua.loadlib(''))
        self.assertTrue(lua.loadlib('string'))
        self.assertTrue(lua.loadlib('table'))
        self.assertEqual(lua.execute(scr), [3])

class f:
    def test_json(self):
        lua = Lua()
        value = json.dumps({'bla': 1, 'foo': [2,3,4]})
        result = lua.execute('cjson')

