import unittest

from pulsar.apps.wsgi import Route


class Routes(unittest.TestCase):

    def testRoot(self):
        r = Route('/')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.rule, '')
        r = Route('////')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.rule, '')
        self.assertEqual(r.match(''), {})
        self.assertEqual(r.match('bee/'), {'__remaining__': 'bee/'})

    def testSimple(self):
        r = Route('bla/')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.level, 1)
        self.assertEqual(len(r.variables), 0)
        self.assertEqual(r.rule, 'bla/')
        self.assertEqual(r.match('bla/'), {})
        self.assertEqual(r.match('bladdd/'), None)
        self.assertEqual(r.match('bla/another/'),
                         {'__remaining__': 'another/'})

    def testSimpleLevel2(self):
        r = Route('bla/foo/')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.level, 2)
        self.assertEqual(len(r.variables), 0)
        self.assertEqual(r.rule, 'bla/foo/')
        self.assertEqual(r.match('bla/'), None)
        self.assertEqual(r.match('bla/foo/'), {})
        self.assertEqual(r.match('bla/fooddd/'), None)
        self.assertEqual(r.match('bla/foo/another/'),
                         {'__remaining__': 'another/'})

    def testMalformedRule1(self):
        self.assertRaises(ValueError, Route, '<bla')
        self.assertRaises(ValueError, Route, '<bla/')
        self.assertRaises(ValueError, Route, 'bla>/')
        self.assertRaises(ValueError, Route, '/<bla')

    def testMalformedRule2(self):
        self.assertRaises(ValueError, Route, '<foo>/<bla')
        self.assertRaises(ValueError, Route, 'ciao/<bla/')
        self.assertRaises(ValueError, Route, 'ahhh>/bla>/')
        self.assertRaises(ValueError, Route, 'ahhh/bla>/')

    def testMalformedRule3(self):
        self.assertRaises(ValueError, Route, '<foo>/<foo>')
        self.assertRaises(ValueError, Route, '<foo>/bla/<foo>')
        self.assertRaises(ValueError, Route, '<foo>/<bla>/<foo>')

    def testStringVariable(self):
        r = Route('<name>/')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.variables, set(['name']))
        self.assertEqual(r.breadcrumbs, ((True, 'name'),))
        self.assertEqual(r.rule, '<name>/')
        self.assertEqual(r.match('bla-foo/'), {'name': 'bla-foo'})
        self.assertEqual(r.match('bla/another/'),
                         {'name': 'bla', '__remaining__': 'another/'})
        self.assertEqual(r.url(name='luca'), '/luca/')

    def test2StringVariables(self):
        r = Route('<name>/<child>/')
        self.assertFalse(r.is_leaf)
        self.assertEqual(r.level, 2)
        self.assertEqual(r.variables, set(['name', 'child']))
        self.assertEqual(r.breadcrumbs, ((True, 'name'), (True, 'child')))
        self.assertEqual(r.rule, '<name>/<child>/')
        self.assertEqual(r.match('bla/foo/'), {'name': 'bla', 'child': 'foo'})
        self.assertEqual(r.match('bla/foo/another/'),
                         {'name': 'bla',
                          'child': 'foo',
                          '__remaining__': 'another/'})
        self.assertRaises(KeyError, r.url, name='luca')
        self.assertEqual(r.url(name='luca', child='joshua'), '/luca/joshua/')

    def test_add_dir_Leaf(self):
        r = Route('bla/')
        self.assertFalse(r.is_leaf)
        r2 = Route('foo')
        self.assertTrue(r2.is_leaf)
        r3 = r + r2
        self.assertEqual(r3.rule, 'bla/foo')
        self.assertTrue(r3.is_leaf)

    def testIntVariable(self):
        r = Route('<int:id>/')
        self.assertEqual(str(r), '/<int:id>/')
        self.assertEqual(r.variables, set(['id']))
        self.assertEqual(r.breadcrumbs, ((True, 'id'),))
        self.assertEqual(r.match('35/'), {'id': 35})
        self.assertEqual(r.url(id=1), '/1/')
        self.assertRaises(ValueError, r.url, id='bla')

    def testIntVariableFixDigits(self):
        r = Route('<int(2):id>/')
        self.assertEqual(str(r), '/<int(2):id>/')
        self.assertEqual(r.variables, set(['id']))
        self.assertEqual(r.breadcrumbs, ((True, 'id'),))
        self.assertEqual(r.match('35/'), {'id': 35})
        self.assertEqual(r.match('355/'), None)
        self.assertEqual(r.match('6/'), None)
        self.assertEqual(r.match('ch/'), None)
        self.assertEqual(r.url(id=13), '/13/')
        self.assertEqual(r.url(id=1), '/01/')
        self.assertRaises(ValueError, r.url, id=134)
        self.assertRaises(ValueError, r.url, id='bl')
        self.assertRaises(ValueError, r.url, id='bla')

    def testIntVariableMinMax(self):
        r = Route('<int(min=1):cid>/')
        self.assertEqual(str(r), '/<int(min=1):cid>/')
        self.assertEqual(r.variables, set(['cid']))
        self.assertEqual(r.breadcrumbs, ((True, 'cid'),))
        self.assertEqual(r.match('1/'), {'cid': 1})
        self.assertEqual(r.match('476876/'), {'cid': 476876})
        self.assertEqual(r.match('0/'), None)
        self.assertEqual(r.match('-5/'), None)
        self.assertEqual(r.url(cid=13), '/13/')
        self.assertEqual(r.url(cid=1), '/1/')
        self.assertRaises(ValueError, r.url, cid=0)
        self.assertRaises(ValueError, r.url, cid=-10)
        self.assertRaises(ValueError, r.url, cid='bla')

    def testPathVaiable(self):
        r = Route('bla/<path:rest>', defaults={'rest': ''})
        self.assertEqual(r.variables, set(['rest']))
        self.assertEqual(r.level, 2)
        self.assertTrue(r.is_leaf)
        self.assertEqual(r.match('bla/a/b/c.html'), {'rest': 'a/b/c.html'})
        self.assertEqual(r.match('bla/'), {'rest': ''})
        self.assertEqual(r.url(rest='a/'), '/bla/a/')
        self.assertEqual(r.url(), '/bla/')

    def testSplitRoot(self):
        r = Route('')
        self.assertEqual(r.level, 0)
        p, l = r.split()
        self.assertFalse(p.is_leaf)
        self.assertEqual(p.path, '/')
        self.assertEqual(l, None)
        r = Route('bla')
        p, l = r.split()
        self.assertFalse(p.is_leaf)
        self.assertTrue(l.is_leaf)
        self.assertEqual(p.path, '/')
        self.assertEqual(l.path, '/bla')
        r = Route('bla/')
        p, l = r.split()
        self.assertFalse(p.is_leaf)
        self.assertFalse(l.is_leaf)
        self.assertEqual(p.path, '/')
        self.assertEqual(l.path, '/bla/')

    def testSplitDir(self):
        r = Route('bla/foo/<id>/pluto/')
        self.assertEqual(r.level, 4)
        p, l = r.split()
        self.assertFalse(p.is_leaf)
        self.assertFalse(l.is_leaf)
        self.assertEqual(p.path, '/bla/foo/<id>/')
        self.assertEqual(l.path, '/pluto/')

    def testSplitLeaf(self):
        r = Route('bla/foo/<id>/pluto/leaf')
        p, l = r.split()
        self.assertFalse(p.is_leaf)
        self.assertTrue(l.is_leaf)
        self.assertEqual(p.path, '/bla/foo/<id>/pluto/')
        self.assertEqual(l.path, '/leaf')

    def testDefaults(self):
        r = Route('bla/<id>/add/<path:path>', {'path': ''})
        self.assertEqual(r.url(id=10), '/bla/10/add/')
        self.assertEqual(r.url(id=10, path='ciao/luca'),
                         '/bla/10/add/ciao/luca')

    def testDefaultsAddition(self):
        r = Route('add/<path:path>', {'path': ''})
        r2 = Route('bla/<id>/') + r
        self.assertEqual(r2.defaults, r.defaults)

    def test_add_string(self):
        r = Route('add/<path:path>', {'path': 'foo'})
        r2 = r + 'bla'
        self.assertEqual(r2.rule, 'add/<path:path>/bla')
        self.assertEqual(str(r2), '/add/<path:path>/bla')
        self.assertNotEqual(r2, '/add/<path:path>/bla')

    def test_empty_url(self):
        r = Route('')
        self.assertEqual(r.rule, '')
        self.assertEqual(r.url(), '/')
        self.assertEqual(r.path, '/')
