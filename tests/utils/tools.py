'''Tests the tools and utilities in pulsar.utils.'''
import os
import unittest

from pulsar import system, get_actor, spawn, send
from pulsar.utils.tools import checkarity, Pidfile, nice_number
from pulsar.apps.test import ActorTestMixin


def f0(a, b):
    pass


def f0_discount(request, a, b):
    pass


def f1(a, b=0):
    pass


def f2(a, **kwargs):
    # This fails curretly
    pass


def arity_check(func, *args, **kwargs):
    discount = kwargs.pop('discount', 0)
    return checkarity(func, args, kwargs, discount=discount)


class TestArityCheck(unittest.TestCase):

    def testArity0(self):
        self.assertEqual(arity_check(f0, 3, 4), None)
        self.assertEqual(arity_check(f0, 3),
                         '"f0" takes 2 parameters. 1 given.')
        self.assertEqual(arity_check(f0),
                         '"f0" takes 2 parameters. 0 given.')
        self.assertEqual(arity_check(f0, 4, 5, 6),
                         '"f0" takes 2 parameters. 3 given.')
        self.assertEqual(arity_check(f0, a=3, b=5), None)
        self.assertEqual(arity_check(f0, a=3, c=5),
                         '"f0" has missing "b" parameter.')
        self.assertEqual(arity_check(f0, a=3, c=5, d=6),
                         '"f0" takes 2 parameters. 3 given.')

    def testArity0WidthDiscount(self):
        f0 = f0_discount
        fname = f0.__name__
        self.assertEqual(arity_check(f0, 3, 4, discount=1), None)
        self.assertEqual(arity_check(f0, 3, discount=1),
                         '"%s" takes 2 parameters. 1 given.' % fname)
        self.assertEqual(arity_check(f0, discount=1),
                         '"%s" takes 2 parameters. 0 given.' % fname)
        self.assertEqual(arity_check(f0, 4, 5, 6, discount=1),
                         '"%s" takes 2 parameters. 3 given.' % fname)
        self.assertEqual(arity_check(f0, a=3, b=5, discount=1), None)
        self.assertEqual(arity_check(f0, a=3, c=5, discount=1),
                         '"%s" has missing "b" parameter.' % fname)
        self.assertEqual(arity_check(f0, a=3, c=5, d=6, discount=1),
                         '"%s" takes 2 parameters. 3 given.' % fname)

    def testArity1(self):
        self.assertEqual(checkarity(f1, (3,), {}), None)
        self.assertEqual(checkarity(f1, (3, 4), {}), None)
        self.assertEqual(checkarity(f1, (), {}),
                         '"f1" takes at least 1 parameters. 0 given.')
        self.assertEqual(checkarity(f1, (4, 5, 6), {}),
                         '"f1" takes at most 2 parameters. 3 given.')
        self.assertEqual(checkarity(f1, (), {'a': 3, 'b': 5}), None)
        self.assertEqual(checkarity(f1, (), {'a': 3, 'c': 5}),
                         '"f1" does not accept "c" parameter.')
        self.assertEqual(checkarity(f1, (), {'a': 3, 'c': 5, 'd': 6}),
                         '"f1" takes at most 2 parameters. 3 given.')

    def testArity2(self):
        self.assertEqual(checkarity(f2, (3,), {}), None)
        self.assertEqual(checkarity(f2, (3,), {'c': 4}), None)
        self.assertEqual(checkarity(f2, (3, 4), {}),
                         '"f2" takes 1 positional parameters. 2 given.')
        self.assertEqual(checkarity(f2, (), {}),
                         '"f2" takes at least 1 parameters. 0 given.')
        self.assertEqual(checkarity(f2, (4, 5, 6), {}),
                         '"f2" takes 1 positional parameters. 3 given.')
        self.assertEqual(checkarity(f2, (), {'a': 3, 'b': 5}), None)
        self.assertEqual(checkarity(f2, (), {'a': 3, 'c': 5}), None)
        self.assertEqual(checkarity(f2, (), {'b': 3, 'c': 5}),
                         '"f2" has missing "a" parameter.')
        self.assertEqual(checkarity(f2, (), {'a': 3, 'c': 5, 'd': 6}), None)


class TestPidfile(ActorTestMixin, unittest.TestCase):
    concurrency = 'process'

    def testCreate(self):
        proxy = yield self.spawn_actor(name='pippo')
        info = yield send(proxy, 'info')
        result = info['actor']
        self.assertTrue(result['is_process'])
        pid = result['process_id']
        #
        p = Pidfile()
        self.assertEqual(p.fname, None)
        self.assertEqual(p.pid, None)
        p.create(pid)
        self.assertTrue(p.fname)
        self.assertEqual(p.pid, pid)
        p1 = Pidfile(p.fname)
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        #
        p1 = Pidfile('bla/ksdcskcbnskcdbskcbksdjcb')
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        p1.unlink()
        p.unlink()
        self.assertFalse(os.path.exists(p.fname))


class TestSystemInfo(unittest.TestCase):

    def testMe(self):
        worker = get_actor()
        info = system.process_info(worker.pid)
        info2 = system.process_info()
        self.assertTrue(isinstance(info, dict))


class TestFunctions(unittest.TestCase):

    def test_convert_bytes(self):
        from pulsar.utils.system import convert_bytes
        self.assertEqual(convert_bytes(None), '#NA')
        self.assertEqual(convert_bytes(4), '4B')
        self.assertEqual(convert_bytes(1024),    '1.0KB')
        self.assertEqual(convert_bytes(1024**2), '1.0MB')
        self.assertEqual(convert_bytes(1024**3), '1.0GB')
        self.assertEqual(convert_bytes(1024**4), '1.0TB')
        self.assertEqual(convert_bytes(1024**5), '1.0PB')
        self.assertEqual(convert_bytes(1024**6), '1.0EB')
        self.assertEqual(convert_bytes(1024**7), '1.0ZB')
        self.assertEqual(convert_bytes(1024**8), '1.0YB')

    def test_nice_number(self):
        self.assertEqual(nice_number(0), 'zero')
        self.assertEqual(nice_number(1), 'one')
        self.assertEqual(nice_number(2), 'two')
        self.assertEqual(nice_number(1, 'bla'), 'one bla')
        self.assertEqual(nice_number(10, 'bla'), 'ten blas')
        self.assertEqual(nice_number(23, 'bla', 'blax'), 'twenty three blax')

    def test_nice_number_large(self):
        self.assertEqual(nice_number(100), 'one hundred')
        self.assertEqual(nice_number(203), 'two hundred and three')
        self.assertEqual(nice_number(4210),
                         'four thousand, two hundred and ten')
        self.assertEqual(nice_number(51345618),
                         'fifty one million, three hundred forty five '
                         'thousand, six hundred and eighteen')
