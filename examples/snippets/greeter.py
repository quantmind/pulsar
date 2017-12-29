import asyncio

from pulsar.api import arbiter, command, Config


names = ['john', 'luca', 'carl', 'jo', 'alex']


@command()
def greetme(request, message):
    echo = 'Hello {}!'.format(message['name'])
    request.actor.logger.warning(echo)
    return echo


class Greeter:

    def __call__(self, arb):
        self._greater_task = arb._loop.create_task(self._work(arb))

    async def _work(self, arb):
        a = await arb.spawn(name='greeter')
        while names:
            name = names.pop()
            await arb.send(a, 'greetme', {'name': name})
            await asyncio.sleep(1)
        arb.stop()


if __name__ == '__main__':
    cfg = Config()
    cfg.parse_command_line()
    arbiter(cfg=cfg, start=Greeter()).start()
