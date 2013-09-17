from pulsar import Deferred
from pulsar.utils.pep import new_event_loop
from pulsar.apps.test import unittest


def async_func(loop, value):
    p = Deferred()
    loop.call_later(0.01, p.callback, value)
    return p

def sub_sub(loop, num):
    a = yield async_func(loop, num)
    b = yield async_func(loop, num)
    yield a+b
 
def sub(loop, num):
    a = yield async_func(loop, num)
    b = yield async_func(loop, num)
    c = yield sub_sub(loop, num)
    yield a+b+c
 
def main(d, loop, num):
    a = yield async_func(loop, num)
    b = yield sub(loop, num)
    c = yield sub(loop, num)
    d.callback(a+b+c)
    
    
class TestCoroutine(unittest.TestCase):
    
    def test_coroutine(self):
        loop = new_event_loop(iothreadloop=False)
        d= Deferred()
        loop.call_soon(main, d, loop, 1)
        loop.run_until_complete(d)
        self.assertEqual(d.result, 9)