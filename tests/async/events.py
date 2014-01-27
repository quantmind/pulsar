import unittest

from pulsar import EventHandler


class TestFailure(unittest.TestCase):

    def test_one_time(self):
        h = EventHandler(one_time_events=('start', 'finish'))
        h.bind_event('finish', lambda f: 'OK')
        result = h.fire_event('finish')
        self.assertTrue(h.event('finish').done())
        self.assertEqual(result, 'OK')

    def test_one_time_error(self):
        h = EventHandler(one_time_events=('start', 'finish'))
        h.bind_event('finish', lambda f: 'OK'+4)
        result = h.fire_event('finish')
        self.assertTrue(h.event('finish').done())
        self.assertTrue(isinstance(result, Failure))
        result.mute()

    def test_bind_events(self):
        h = EventHandler(one_time_events=('start', 'finish'))
        h.bind_events(foo=3, bla=6)
        self.assertFalse(h.events['start'].events.has_callbacks())
        self.assertFalse(h.events['finish'].events.has_callbacks())
        h.bind_events(start=lambda r: r+1,
                      finish=[(lambda r: r+1, lambda f: f.mute())])
        self.assertTrue(h.events['start'].events.has_callbacks())
        self.assertTrue(h.events['finish'].events.has_callbacks())
        self.assertEqual(h.fire_event('start', 2), 3)
        failure = maybe_failure(ValueError('test'))
        self.assertEqual(h.fire_event('finish', failure), failure)
        self.assertTrue(failure.logged)
