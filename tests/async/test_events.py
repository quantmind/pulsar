import unittest

from pulsar.api import EventHandler


class Handler(EventHandler):
    ONE_TIME_EVENTS = ('start', 'finish')


class TestFailure(unittest.TestCase):

    def test_one_time(self):
        h = Handler()
        self.assertTrue(h.event('finish').onetime())
        h.event('finish').fire()
        self.assertTrue(h.event('finish').fired())

    async def test_one_time_error(self):
        h = Handler()
        h.bind_event('finish', lambda f, exc=None: 'OK'+4)
        result = await h.fire_event('finish', 3)
        self.assertTrue(h.event('finish').done())
        self.assertEqual(result, 3)

    async def test_bind_events(self):
        h = Handler()
        h.bind_events(foo=3, bla=6)
        self.assertFalse(h.events['start'].handlers)
        self.assertFalse(h.events['finish'].handlers)
        h.bind_events(start=lambda r, exc=None: r+1,
                      finish=lambda r, exc=None: r+1)
        self.assertTrue(h.events['start'].handlers)
        self.assertTrue(h.events['finish'].handlers)
        result = await h.fire_event('start', 2)
        self.assertEqual(result, 2)

    def test_remove_callback(self):
        h = Handler()

        def cbk(_, **kw):
            return kw

        h.bind_event('many', cbk)
        self.assertTrue(h.event('many'))
        self.assertEqual(h.remove_callback('bla', cbk), None)
        self.assertEqual(h.remove_callback('many', cbk), 1)
        self.assertEqual(h.remove_callback('many', cbk), 0)
        self.assertEqual(h.event('many').handlers, [])
