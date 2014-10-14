import unittest

from pulsar.apps import wsgi


class TestHtml(unittest.TestCase):

    def test_html_factory(self):
        input = wsgi.html_factory('input', type='text')
        h = input(value='bla')
        self.assertTrue(h)
        self.assertEqual(h.attr('value'), 'bla')
        self.assertEqual(h.attr('type'), 'text')
        text = h.render()
        self.assertTrue(" type='text'" in text)
        self.assertTrue(" value='bla'" in text)

    def testEmpty(self):
        c = wsgi.Html('div')
        self.assertEqual(c._attr, None)
        self.assertEqual(c._classes, None)
        self.assertEqual(c._data, None)
        self.assertEqual(c._css, None)

    def test_html_repr(self):
        c = wsgi.Html('div', cn='bla', charset='utf-16')
        self.assertEqual(c.content_type, 'text/html; charset=utf-16')
        self.assertEqual(str(c), "<div class='bla'>")
        c = wsgi.Html(None, cn='bla')
        self.assertEqual(c.tag, None)
        self.assertEqual(str(c), "Html")

    def testClass(self):
        c = wsgi.Html('div').addClass('ciao').addClass('pippo')
        self.assertTrue(c.hasClass('ciao'))
        self.assertTrue(c.hasClass('pippo'))
        f = c.flatatt()
        self.assertTrue(f in (" class='ciao pippo'",
                              " class='pippo ciao'"))
        c = wsgi.Html('div').addClass('ciao pippo bla')
        self.assertTrue(c.hasClass('bla'))

    def testClassList(self):
        c = wsgi.Html('div', cn=['ciao', 'pippo', 'ciao'])
        self.assertEqual(len(c._classes), 2)
        self.assertTrue('ciao' in c._classes)
        self.assertTrue('pippo' in c._classes)
        self.assertTrue(c.hasClass('pippo'))

    def testRemoveClass(self):
        c = wsgi.Html('div', cn=['ciao', 'pippo', 'ciao', 'foo'])
        self.assertEqual(len(c._classes), 3)
        c.removeClass('sdjkcbhjsd smdhcbjscbsdcsd')
        self.assertEqual(len(c._classes), 3)
        c.removeClass('ciao sdjkbsjc')
        self.assertEqual(len(c._classes), 2)
        c.removeClass('pippo foo')
        self.assertEqual(len(c._classes), 0)

    def testData(self):
        c = wsgi.Html('div')
        c.data('food', 'pasta').data('city', 'Rome')
        self.assertEqual(c.data('food'), 'pasta')
        self.assertEqual(c.data('city'), 'Rome')
        f = c.flatatt()
        self.assertTrue(f in (" data-food='pasta' data-city='Rome'",
                              " data-city='Rome' data-food='pasta'"))
        c.data('food', 'risotto')
        self.assertEqual(c.data('food'), 'risotto')

    def testNestedData(self):
        random = ['bla', 3, 'foo']
        table = {'name': 'path',
                 'resizable': True}
        c = wsgi.Html('div')
        r = c.data({'table': table, 'random': random})
        self.assertEqual(r, c)
        self.assertEqual(c.data('table')['name'], 'path')
        self.assertEqual(c.data('table')['resizable'], True)
        self.assertEqual(c.data('random')[0], 'bla')
        attr = c.flatatt()
        self.assertTrue('data-table' in attr)
        c.data('table', {'resizable': False, 'rows': 40})
        self.assertEqual(c.data('table')['name'], 'path')
        self.assertEqual(c.data('table')['resizable'], False)
        self.assertEqual(c.data('table')['rows'], 40)

    def testEmptyAttr(self):
        c = wsgi.Html('input', type='text')
        c.attr('value', None)
        self.assertEqual(c.attr('value'), None)
        self.assertEqual(c.flatatt(), " type='text'")
        c.attr('value', '')
        self.assertEqual(c.attr('value'), '')
        self.assertTrue(" value=''" in c.flatatt())
        c.attr('value', 0)
        self.assertTrue(" value='0'" in c.flatatt())
        self.assertEqual(c.attr('value'), 0)

    def test_option_empty_attribute(self):
        opt = wsgi.Html('option', '--------', value='')
        self.assertEqual(opt.attr('value'), '')
        text = opt.render()
        self.assertTrue(" value=''" in text)

    def test_textarea_value_attribute(self):
        opt = wsgi.Html('textarea', 'Hello World!')
        self.assertEqual(opt.attr('value'), None)
        self.assertEqual(opt.get_form_value(), 'Hello World!')
        text = opt.render()
        self.assertTrue("Hello World!" in text)

    def testHide(self):
        c = wsgi.Html('div', 'foo').hide()
        self.assertEqual(c.flatatt(), " style='display:none;'")
        c.show()
        self.assertEqual(c.flatatt(), "")


class TestWidgets(unittest.TestCase):

    def test_ancor(self):
        a = wsgi.Html('a', 'kaput', cn='bla', href='/abc/')
        self.assertEqual(a.attr('href'), '/abc/')
        ht = a.render()
        self.assertTrue('>kaput</a>' in ht)
        a = wsgi.Html('a', xxxx='ciao')
        self.assertTrue('xxxx' in a.attr())

    def testList(self):
        ul = wsgi.Html('ul')
        self.assertEqual(ul.tag, 'ul')
        self.assertEqual(len(ul.children), 0)
        ul = wsgi.Html('ul', 'a list item', 'another one')
        self.assertEqual(len(ul.children), 2)
        ht = ul.render()
        self.assertTrue('<ul>' in ht)
        self.assertTrue('</ul>' in ht)
        self.assertTrue('<li>a list item</li>' in ht)
        self.assertTrue('<li>another one</li>' in ht)

    def test_simple(self):
        html = wsgi.HtmlDocument()
        self.assertEqual(len(html.head.children), 5)
        self.assertEqual(len(html.body.children), 0)

    def testHead(self):
        html = wsgi.HtmlDocument()
        head = html.head
        self.assertTrue(head.meta)
        self.assertTrue(head.links)
        self.assertTrue(head.scripts)

    def test_meta(self):
        html = wsgi.HtmlDocument()
        meta = html.head.meta
        self.assertEqual(len(meta.children), 1)
        self.assertEqual(meta.tag, None)
        html.head.add_meta(name='bla')
        self.assertEqual(len(meta.children), 2)
        text = meta.render()
        self.assertEqual(text, "<meta charset='utf-8'>\n<meta name='bla'>\n")

    def test_document_empty_body(self):
        m = wsgi.HtmlDocument(title='test', bla='foo')
        self.assertTrue(m.head)
        self.assertTrue(m.body)
        self.assertEqual(m.head.title, 'test')
        txt = m.render()
        self.assertEqual(txt,
                         '\n'.join(('<!DOCTYPE html>',
                                    "<html bla='foo'>",
                                    '<head>',
                                    '<title>test</title>',
                                    "<meta charset='utf-8'>",
                                    '</head>',
                                    '<body></body>',
                                    '</html>')))

    def test_document(self):
        m = wsgi.HtmlDocument(title='test')
        m.body.append(wsgi.Html('div', 'this is a test'))
        txt = m.render()
        self.assertEqual(txt,
                         '\n'.join(['<!DOCTYPE html>',
                                    '<html>',
                                    '<head>',
                                    '<title>test</title>',
                                    "<meta charset='utf-8'>",
                                    '</head>',
                                    '<body>',
                                    '<div>this is a test</div>'
                                    '</body>',
                                    '</html>']))
