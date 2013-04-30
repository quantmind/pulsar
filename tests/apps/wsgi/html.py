from pulsar.apps import wsgi
from pulsar.apps.test import unittest


class TestHtmlFactory(unittest.TestCase):
    
    def test_html_factory(self):
        input = wsgi.html_factory('input', type='text')
        h = input(value='bla')
        self.assertTrue(h)
        self.assertEqual(h.attr('value'), 'bla')
        self.assertEqual(h.attr('type'), 'text')
        text = h.render()
        self.assertEqual(text, "<input type='text' value='bla'>")
        
        
class TestAttributes(unittest.TestCase):

    def testWidegtRepr(self):
        c = w('div', cn='bla')
        self.assertEqual(c.content_type, 'text/html')
        self.assertEqual(str(c), "<div class='bla'>")
        c = w(cn='bla')
        self.assertEqual(c.tag, None)
        self.assertEqual(str(c), 'Widget(WidgetMaker)')

    def testClass(self):
        c = w('div').addClass('ciao').addClass('pippo')
        self.assertTrue('ciao' in c.classes)
        self.assertTrue('pippo' in c.classes)
        f = c.flatatt()
        self.assertTrue(f in (" class='ciao pippo'",
                              " class='pippo ciao'"))
        c = w('div').addClass('ciao pippo bla')
        self.assertTrue(c.hasClass('bla'))
        
    def testClassList(self):
        c = w('div', cn=['ciao', 'pippo', 'ciao'])
        self.assertEqual(len(c.classes), 2)
        self.assertTrue('ciao' in c.classes)
        self.assertTrue('pippo' in c.classes)
        
    def testRemoveClass(self):
        c = w('div', cn=['ciao', 'pippo', 'ciao', 'foo'])
        self.assertEqual(len(c.classes), 3)
        c.removeClass('sdjkcbhjsd smdhcbjscbsdcsd')
        self.assertEqual(len(c.classes), 3)
        c.removeClass('ciao sdjkbsjc')
        self.assertEqual(len(c.classes), 2)
        c.removeClass('pippo foo')
        self.assertEqual(len(c.classes), 0)
        
    def testData(self):
        c = w('div')
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
        c = w('div').data({'table': table, 'random': random})
        self.assertEqual(c.data('table')['name'], 'path')
        self.assertEqual(c.data('table')['resizable'], True)
        self.assertEqual(c.data('random')[0], 'bla')
        attr = c.flatatt()
        self.assertTrue('data-table' in attr)
        c.data('table', {'resizable':False, 'rows':40})
        self.assertEqual(c.data('table')['name'], 'path')
        self.assertEqual(c.data('table')['resizable'], False)
        self.assertEqual(c.data('table')['rows'], 40)

    def testEmptyAttr(self):
        c = w('input:text')
        c.attr('value', None)
        self.assertEqual(c.attr('value'), None)
        self.assertEqual(c.flatatt(), " type='text'")
        c.attr('value', '')
        self.assertEqual(c.attr('value'),'')
        self.assertTrue(" value=''" in c.flatatt())
        c.attr('value', 0)
        self.assertTrue(" value='0'" in c.flatatt())
        self.assertEqual(c.attr('value'), 0)

    def testEmptyAttribute(self):
        opt = w('option', '--------', value='')
        self.assertEqual(opt.attr('value'), '')
        text = opt.render()
        self.assertTrue(" value=''" in text)
        
    def testHide(self):
        c = w('div', 'foo').hide()
        self.assertEqual(c.flatatt(), " style='display:none;'")
        c.show()
        self.assertEqual(c.flatatt(), "")
        
        
class TestWidgets(unittest.TestCase):
    
    def testAncor(self):
        a = w('a', 'kaput', cn='bla', href='/abc/')
        self.assertEqual(a.attr('href'), '/abc/')
        ht = a.render()
        self.assertTrue('>kaput</a>' in ht)
        a = w('a', xxxx='ciao')
        self.assertFalse('xxxx' in a.attr())
        self.assertEqual(a.data('xxxx'), 'ciao')
    
    def testList(self):
        ul = lux.Ulist()
        self.assertEqual(ul.tag, 'ul')
        self.assertEqual(len(ul.children), 0)
        ul = lux.Ulist('a list item', 'another one')
        self.assertEqual(len(ul.children), 2)
        ht = ul.render()
        self.assertTrue('<ul>' in ht)
        self.assertTrue('</ul>' in ht)
        self.assertTrue('<li>a list item</li>' in ht)
        self.assertTrue('<li>another one</li>' in ht)
        
        
class TestHtmlDocument(unittest.TestCase):
    
    def testSimple(self):
        html = lux.Html()
        self.assertEqual(len(html.head.children), 3)
        self.assertEqual(len(html.body.children), 0)
        
    def testHead(self):
        html = lux.Html()
        head = html.head
        self.assertTrue(head.meta)
        self.assertTrue(head.links)
        self.assertTrue(head.scripts)
        
    def testMeta(self):
        html = lux.Html()
        meta = html.head.meta
        self.assertEqual(len(meta.children), 0)
        self.assertEqual(meta.tag, None)
        meta.append(lux.Meta(name='bla'))
        self.assertEqual(len(meta.children), 1)
        text = meta.render()
        self.assertEqual(text, "<meta name='bla'>")
