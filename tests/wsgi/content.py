import unittest
from asyncio import Future

from pulsar.apps import wsgi
from pulsar.utils.system import json


class TestAsyncContent(unittest.TestCase):

    def test_string(self):
        a = wsgi.AsyncString('Hello')
        self.assertEqual(a.render(), 'Hello')
        self.assertRaises(RuntimeError, a.render)

    def test_simple_json(self):
        response = wsgi.Json({'bla': 'foo'})
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type,
                         'application/json; charset=utf-8')
        self.assertFalse(response.as_list)
        self.assertEqual(response.render(), json.dumps({'bla': 'foo'}))

    def test_simple_json_as_list(self):
        response = wsgi.Json({'bla': 'foo'}, as_list=True)
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type,
                         'application/json; charset=utf-8')
        self.assertTrue(response.as_list)
        self.assertEqual(response.render(), json.dumps([{'bla': 'foo'}]))

    def test_json_with_async_string(self):
        astr = wsgi.AsyncString('ciao')
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type,
                         'application/json; charset=utf-8')
        self.assertEqual(response.render(), json.dumps({'bla': 'ciao'}))

    def test_json_with_async_string2(self):
        d = Future()
        astr = wsgi.AsyncString(d)
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        result = response.render()
        self.assertIsInstance(result, Future)
        d.set_result('ciao')
        result = yield result
        self.assertEqual(result, json.dumps({'bla': 'ciao'}))

    def test_append_self(self):
        root = wsgi.AsyncString()
        self.assertEqual(root.parent, None)
        root.append(root)
        self.assertEqual(root.parent, None)
        self.assertEqual(len(root.children), 0)

    def test_append(self):
        root = wsgi.AsyncString()
        child1 = wsgi.AsyncString()
        child2 = wsgi.AsyncString()
        root.append(child1)
        self.assertEqual(child1.parent, root)
        self.assertEqual(len(root.children), 1)
        root.prepend(child2)
        self.assertEqual(child2.parent, root)
        self.assertEqual(len(root.children), 2)

    def test_append_parent(self):
        root = wsgi.AsyncString()
        child1 = wsgi.AsyncString()
        child2 = wsgi.AsyncString()
        root.append(child1)
        root.append(child2)
        self.assertEqual(len(root.children), 2)
        child1.append(root)
        self.assertEqual(child1.parent, None)
        self.assertEqual(root.parent, child1)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(len(child1.children), 1)

    def test_append_parent_with_parent(self):
        root = wsgi.AsyncString()
        child1 = wsgi.AsyncString()
        child2 = wsgi.AsyncString()
        child3 = wsgi.AsyncString()
        root.append(child1)
        child1.append(child2)
        child1.append(child3)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(len(child1.children), 2)
        child2.append(child1)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(root.children[0], child2)
        self.assertEqual(len(child2.children), 1)
        self.assertEqual(child1.parent, child2)
        self.assertEqual(child2.parent, root)

    def test_change_parent(self):
        root = wsgi.AsyncString()
        child1 = wsgi.AsyncString()
        child2 = wsgi.AsyncString()
        child3 = wsgi.AsyncString()
        root.append(child1)
        child1.append(child2)
        child1.append(child3)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(len(child1.children), 2)
        root.append(child3)
        self.assertEqual(len(root.children), 2)
        self.assertEqual(len(child1.children), 1)

    def test_remove_valueerror(self):
        root = wsgi.AsyncString()
        child1 = wsgi.AsyncString()
        self.assertEqual(len(root.children), 0)
        root.remove(child1)
        self.assertEqual(len(root.children), 0)
        child1.append_to(root)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(child1.parent, root)

    def test_remove_all(self):
        root = wsgi.Html('div')
        child1 = wsgi.Html('div')
        root.append(child1)
        root.append('ciao')
        self.assertEqual(len(root.children), 2)
        root.remove_all()
        self.assertEqual(len(root.children), 0)

    def test_media_path(self):
        media = wsgi.Scripts('/media/',
                             known_libraries={'jquery':
                                              'http://bla.foo/jquery'})
        self.assertTrue(media.is_relative('bla/test.js'))
        path = media.absolute_path('bla/foo.js')
        self.assertEqual(path, '/media/bla/foo.js')
        self.assertEqual(media.absolute_path('/bla/foo.js'), '/bla/foo.js')
        self.assertEqual(media.absolute_path('jquery'),
                         'http://bla.foo/jquery.js')

    def test_media_minified(self):
        media = wsgi.Css('/media/', minified=True)
        self.assertEqual(media.absolute_path('bla/foo.css'),
                         '/media/bla/foo.min.css')
        self.assertEqual(media.absolute_path('bla/foo.min.css'),
                         '/media/bla/foo.min.css')

    def test_html_doc_media(self):
        doc = wsgi.HtmlDocument(media_path='/foo/')
        self.assertEqual(doc.head.scripts.media_path, '/foo/')
        self.assertEqual(doc.head.links.media_path, '/foo/')
        doc = doc(title='ciao', media_path='/assets/')
        self.assertEqual(doc.head.title, 'ciao')
        self.assertEqual(doc.head.scripts.media_path, '/assets/')
        self.assertEqual(doc.head.links.media_path, '/assets/')
