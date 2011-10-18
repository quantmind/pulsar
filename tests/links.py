'''Tests actor links api.'''
from time import sleep
import unittest as test

import pulsar
from pulsar.apps.test import AsyncTestCaseMixin


def test_callable(a):
    return 'Hi from {0}'.format(a.aid)


class TestActorLinks(test.TestCase, AsyncTestCaseMixin):
    impl = 'thread'
    
    def testArbiterLink(self):
        arbiter = pulsar.arbiter()
        yield self.spawn(impl = self.impl)
        a = self.a
        #
        # Create a link to actor a
        link = pulsar.ActorLink(a.aid)
        self.assertEqual(link.name,a.aid)
        #
        pa = link.proxy(arbiter)
        self.assertEqual(pa.aid,a.aid)
        #
        cbk = link.get_callback(arbiter, 'run')
        self.assertEqual(cbk.action,'run')
        self.assertEqual(cbk.sender, arbiter)
        #
        # Run the bad call
        r = cbk()
        self.assertRaises(pulsar.AlreadyCalledError,cbk)
        yield self.assertFailure(r,TypeError)
        #
        # Run the correct call
        r = link(arbiter, 'run', test_callable)
        yield r
        result = r.result
        self.assertEqual(result,'Hi from {0}'.format(a.aid))
        #
        yield self.stop()
    testArbiterLink.run_on_arbiter = True
    
    def testLinkNotCalled(self):
        arbiter = pulsar.arbiter()
        yield self.spawn(impl = self.impl)
        a = self.a
        #
        # Create a link to actor a
        link = pulsar.ActorLink(a.aid)
        cbk = link.get_callback(arbiter, 'run', test_callable)
        result = cbk.result()
        self.assertTrue(result.called)
        self.assertEqual(result.result,None)
        #
        yield self.stop()
    testLinkNotCalled.run_on_arbiter = True
    
    
class TestActorLinksProcess(TestActorLinks):
    impl = 'process'
    