import unittest

from pulsar import create_future, get_event_loop


DELAY = 0


def async_func(loop, value):
    p = create_future(loop)
    loop.call_later(DELAY, p.set_result, value)
    return p


async def sub_sub(loop, num):
    a = await async_func(loop, num)
    b = await async_func(loop, num)
    return a+b


async def sub(loop, num):
    return (
        await async_func(loop, num) +
        await async_func(loop, num) +
        await sub_sub(loop, num)
    )

async def main(num, loop=None):
    loop = loop or get_event_loop()
    a = await async_func(loop, num)
    b = await sub(loop, num)
    c = await sub(loop, num)
    return a+b+c


class TestCoroutine(unittest.TestCase):
    __benchmark__ = True
    __number__ = 1000

    async def test_coroutine(self):
        result = await main(1)
        self.assertEqual(result, 9)
