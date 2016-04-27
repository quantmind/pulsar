import unittest

from pulsar import Future
from pulsar.apps import wsgi
from pulsar.utils.system import json


class TestAsyncContent(unittest.TestCase):

    def test_string(self):
        a = wsgi.String('Hello')
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
        astr = wsgi.String('ciao')
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type,
                         'application/json; charset=utf-8')
        self.assertEqual(response.render(), json.dumps({'bla': 'ciao'}))

    async def test_json_with_async_string2(self):
        d = Future()
        astr = wsgi.String(d)
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        result = response.render()
        self.assertIsInstance(result, Future)
        d.set_result('ciao')
        result = await result
        self.assertEqual(result, json.dumps({'bla': 'ciao'}))

    def test_append_self(self):
        root = wsgi.String()
        self.assertEqual(root.parent, None)
        root.append(root)
        self.assertEqual(root.parent, None)
        self.assertEqual(len(root.children), 0)

    def test_append(self):
        root = wsgi.String()
        child1 = wsgi.String()
        child2 = wsgi.String()
        root.append(child1)
        self.assertEqual(child1.parent, root)
        self.assertEqual(len(root.children), 1)
        root.prepend(child2)
        self.assertEqual(child2.parent, root)
        self.assertEqual(len(root.children), 2)

    def test_append_parent(self):
        root = wsgi.String()
        child1 = wsgi.String()
        child2 = wsgi.String()
        root.append(child1)
        root.append(child2)
        self.assertEqual(len(root.children), 2)
        child1.append(root)
        self.assertEqual(child1.parent, None)
        self.assertEqual(root.parent, child1)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(len(child1.children), 1)

    def test_append_parent_with_parent(self):
        root = wsgi.String()
        child1 = wsgi.String()
        child2 = wsgi.String()
        child3 = wsgi.String()
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
        root = wsgi.String()
        child1 = wsgi.String()
        child2 = wsgi.String()
        child3 = wsgi.String()
        root.append(child1)
        child1.append(child2)
        child1.append(child3)
        self.assertEqual(len(root.children), 1)
        self.assertEqual(len(child1.children), 2)
        root.append(child3)
        self.assertEqual(len(root.children), 2)
        self.assertEqual(len(child1.children), 1)

    def test_remove_valueerror(self):
        root = wsgi.String()
        child1 = wsgi.String()
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
        media = wsgi.Scripts('/media/')
        self.assertTrue(media.is_relative('bla/test.js'))
        path = media.absolute_path('bla/foo.js')
        self.assertEqual(path, '/media/bla/foo.js')
        self.assertEqual(media.absolute_path('/bla/foo.js'), '/bla/foo.js')

    def test_links_minified(self):
        media = wsgi.Links('/media/', minified=True)
        self.assertEqual(media.absolute_path('bla/foo'),
                         '/media/bla/foo.min.css')
        self.assertEqual(media.absolute_path('bla/foo.min.css'),
                         '/media/bla/foo.min.css')
        self.assertEqual(media.absolute_path('bla/foo.css'),
                         '/media/bla/foo.css')

    def test_scripts_minified(self):
        media = wsgi.Scripts('/media/', minified=True)
        self.assertEqual(media.absolute_path('bla/foo'),
                         '/media/bla/foo.min.js')
        self.assertEqual(media.absolute_path('bla/foo.min.js'),
                         '/media/bla/foo.min.js')
        self.assertEqual(media.absolute_path('bla/foo.js'),
                         '/media/bla/foo.js')

    def test_html_doc_media(self):
        doc = wsgi.HtmlDocument(media_path='/foo/')
        self.assertEqual(doc.head.scripts.media_path, '/foo/')
        self.assertEqual(doc.head.links.media_path, '/foo/')
        doc.head.title = 'ciao'
        doc.head.media_path = '/assets/'
        self.assertEqual(doc.head.title, 'ciao')
        self.assertEqual(doc.head.scripts.media_path, '/assets/')
        self.assertEqual(doc.head.links.media_path, '/assets/')

    def test_link_condition(self):
        links = wsgi.Links('/media/')
        links.append('bla.css', condition='IE 6')
        html = links.render()
        lines = html.split('\n')
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], '<!--[if IE 6]>')
        self.assertEqual(lines[1], ("<link href='/media/bla.css' "
                                    "rel='stylesheet' type='text/css'>"))
        self.assertEqual(lines[2], '<![endif]-->')
        self.assertEqual(lines[3], '')

    def test_script(self):
        links = wsgi.Scripts('/static/')
        links.require.append('require')
        html = links.render()
        lines = html.split('\n')
        self.assertEqual(len(lines), 4)
