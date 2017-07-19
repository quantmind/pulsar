import unittest

from pulsar.api import EventHandler


class Handler(EventHandler):
    ONE_TIME_EVENTS = ('start', 'finish')


class TestFailure(unittest.TestCase):

    def test_one_time(self):
        h = Handler()
        e = h.event('finish')
        self.assertTrue(e.onetime())
        e.fire()
        self.assertTrue(e.fired())
        self.assertEqual(e.handlers(), None)

    def test_one_time_error(self):
        h = Handler()
        h.event('finish').bind(lambda f, exc=None: 'OK' + 4)
        with self.assertRaises(TypeError):
            h.event('finish').fire()

    def test_bind_events(self):
        h = Handler()
        h.bind_events({'foo': 3, 'bla': 6})
        self.assertFalse(h.event('start').handlers())
        self.assertFalse(h.event('finish').handlers())
        h.bind_events({'start': lambda r, data=None, exc=None: data + 1,
                       'finish': lambda r, data=None, exc=None: data + 1})
        self.assertTrue(h.event('start').handlers())
        self.assertTrue(h.event('finish').handlers())
        h.event('start').fire(data=1)
        self.assertTrue(h.event('start').fired())
        self.assertRaises(RuntimeError, h.event('start').bind, lambda: None)

    def test_unbind(self):
        h = Handler()

        def cbk(_, **kw):
            return kw

        h.event('many').bind(cbk)
        e = h.event('many')
        self.assertTrue(e)
        self.assertEqual(h.event('foo').unbind(cbk), 0)
        self.assertEqual(e.unbind(cbk), 1)
        self.assertEqual(e.unbind(cbk), 0)
        self.assertEqual(e.handlers(), [])

    def test_copy_many_times_events(self):
        h = EventHandler()

        def cbk(_, **kw):
            return kw

        h.event('start').bind(cbk)
        self.assertEqual(h.event('start').onetime(), False)
        h2 = Handler()
        self.assertEqual(h2.event('start').onetime(), True)
        h2.copy_many_times_events(h)
        self.assertEqual(h2.event('start').handlers(), [cbk])
