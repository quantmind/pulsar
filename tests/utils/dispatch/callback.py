'''Test the one time callback'''
from pulsar.utils.events import Signal
from pulsar.apps.test import unittest

a_signal = Signal(providing_args=["val"])


class DispatcherTests(unittest.TestCase):

    def callback(self, signal=None, sender=None, **kwargs):
        self.result = (signal, sender, kwargs)

    def testCallback(self):
        self.assertEqual(len(a_signal.receivers), 0)
        a_signal.add_callback(self.callback)
        self.assertEqual(len(a_signal.receivers), 1)
        # now send
        a_signal.send(self, val='ok')
        self.assertTrue(self.result)
        self.assertEqual(self.result[0], a_signal)
        self.assertEqual(self.result[1], self)
        self.assertEqual(self.result[2], {'val': 'ok'})
        # the callback has gone
        self.assertEqual(len(a_signal.receivers), 0)

