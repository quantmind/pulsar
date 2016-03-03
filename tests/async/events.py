import unittest
import asyncio

from pulsar import EventHandler, get_event_loop


class Handler(EventHandler):

    def __init__(self, **kw):
        self._loop = get_event_loop()
        super().__init__(self._loop, **kw)


class TestFailure(unittest.TestCase):

    @asyncio.coroutine
    def test_one_time(self):
        h = Handler(one_time_events=('start', 'finish'))
        h.bind_event('finish', lambda f, exc=None: 'OK')
        result = yield from h.fire_event('finish', 'foo')
        self.assertTrue(h.event('finish').done())
        self.assertEqual(result, 'foo')

    @asyncio.coroutine
    def test_one_time_error(self):
        h = Handler(one_time_events=('start', 'finish'))
        h.bind_event('finish', lambda f, exc=None: 'OK'+4)
        result = yield from h.fire_event('finish', 3)
        self.assertTrue(h.event('finish').done())
        self.assertEqual(result, 3)

    @asyncio.coroutine
    def test_bind_events(self):
        h = Handler(one_time_events=('start', 'finish'))
        h.bind_events(foo=3, bla=6)
        self.assertFalse(h.events['start'].handlers)
        self.assertFalse(h.events['finish'].handlers)
        h.bind_events(start=lambda r, exc=None: r+1,
                      finish=lambda r, exc=None: r+1)
        self.assertTrue(h.events['start'].handlers)
        self.assertTrue(h.events['finish'].handlers)
        result = yield from h.fire_event('start', 2)
        self.assertEqual(result, 2)

    def test_remove_callback(self):
        h = Handler(one_time_events=('start', 'finish'))

        def cbk(_, **kw):
            return kw

        h.bind_event('many', cbk)
        self.assertTrue(h.event('many'))
        self.assertEqual(h.remove_callback('bla', cbk), None)
        self.assertEqual(h.remove_callback('many', cbk), 1)
        self.assertEqual(h.remove_callback('many', cbk), 0)
        self.assertEqual(h.event('many').handlers, [])
