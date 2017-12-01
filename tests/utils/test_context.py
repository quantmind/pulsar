"""Tests task context
"""
import unittest
import asyncio

from pulsar.api import context


class TestContextEmpty(unittest.TestCase):

    def test_get(self):
        self.assertEqual(context.get('foo'), None)

    def test_stack_get(self):
        self.assertEqual(context.stack_get('foo'), None)


class TestContext(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        context.setup()

    @classmethod
    def tearDownClass(cls):
        context.remove()

    def test_task_factory(self):
        self.assertEqual(asyncio.get_event_loop().get_task_factory(), context)

    async def test_set_get_pop(self):
        context.set('foo', 5)
        self.assertEqual(context.get('foo'), 5)
        self.assertEqual(context.pop('foo'), 5)
        self.assertEqual(context.get('foo'), None)

    async def test_set_get_pop_nested(self):
        context.set('foo', 5)
        self.assertEqual(context.get('foo'), 5)
        await asyncio.get_event_loop().create_task(self.nested())
        self.assertEqual(context.get('foo'), 5)
        self.assertEqual(context.get('bla'), None)

    async def test_stack(self):
        with context.begin(text='ciao', planet='mars'):
            self.assertEqual(context.stack_get('text'), 'ciao')
            self.assertEqual(context.stack_get('planet'), 'mars')
        self.assertEqual(context.stack_get('text'), None)
        self.assertEqual(context.stack_get('planet'), None)

    def test_typeerror(self):
        with self.assertRaises(TypeError):
            with context.begin(1, 2):
                pass

    async def nested(self):
        context.set('bla', 7)
        self.assertEqual(context.get('bla'), 7)
        self.assertEqual(context.get('foo'), 5)
        context.set('foo', 8)
        self.assertEqual(context.get('foo'), 8)
