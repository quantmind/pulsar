'''
The dining philosophers_ problem is an example problem often used in
concurrent algorithm design to illustrate synchronisation issues and
techniques for resolving them.

The problem
===================

Five silent philosophers sit at a round table with each a bowl of spaghetti.
A fork ``f`` is placed between each pair of adjacent philosophers ``P``::


         P     P
         O  f  O
        f       f
     P O         O P
         f     f
            O
            P

Each philosopher ``P`` must alternately think and eat from his bowl ``O``.
Eating is not limited by the amount of spaghetti left: assume an infinite
supply.

However, a philosopher can only eat while holding both the fork ``f`` to
the left and the fork to the right.
Each philosopher can pick up an adjacent fork, when available, and put it down,
when holding it. These are separate actions: forks must be picked up and put
down one by one.

This implementation will just work. No starvation or dead-lock.

There are two parameters:

* Average eating period, the higher the more time is spend eating.
* Average waiting period, the higher the more frequent philosophers
  get a chance to eat.

To run the example, type::

    pulsar manage.py

Implementation
=====================

.. autoclass:: DiningPhilosophers
   :members:
   :member-order: bysource


.. _philosophers: http://en.wikipedia.org/wiki/Dining_philosophers_problem

'''
import random
from asyncio import sleep

from pulsar.api import command, Setting, Config, Application
from pulsar.utils.config import validate_pos_float


###########################################################################
#    EXTRA COMMAND LINE PARAMETERS
class PhilosophersSetting(Setting):
    virtual = True
    app = 'philosophers'
    section = "Socket Servers"


class EatingPeriod(PhilosophersSetting):
    flags = ["--eating-period"]
    validator = validate_pos_float
    default = 2
    desc = """The average period of eating for a philosopher."""


class WaitingPeriod(PhilosophersSetting):
    flags = ["--waiting-period"]
    validator = validate_pos_float
    default = 2
    desc = """The average period of waiting for a missing fork."""


###########################################################################
#    PULSAR COMMANDS FOR DINING PHILOSOPHERS
@command(ack=False)
def putdown_fork(request, fork):
    self = request.actor.app
    try:
        self.not_available_forks.remove(fork)
    except KeyError:
        self.logger.error('Putting down a fork which was already available')


@command()
def pickup_fork(request, fork_right):
    self = request.actor.app
    num_philosophers = self.cfg.workers
    fork_left = fork_right - 1
    if fork_left == 0:
        fork_left = num_philosophers
    for fork in (fork_right, fork_left):
        if fork not in self.not_available_forks:
            # Fork is available, send it to the philosopher
            self.not_available_forks.add(fork)
            return fork


############################################################################
#    DINING PHILOSOPHERS APP
class DiningPhilosophers(Application):
    description = ('Dining philosophers sit at a table around a bowl of '
                   'spaghetti and waits for available forks.')
    cfg = Config(workers=5, apps=['philosophers'])
    eating = None

    def monitor_start(self, monitor):
        self.not_available_forks = set()

    def worker_start(self, philosopher, exc=None):
        self._loop = philosopher._loop
        self.eaten = 0
        self.thinking = 0
        self.started_waiting = 0
        self.forks = []
        self.eating = philosopher._loop.create_task(self.eat(philosopher))

    def worker_stopping(self, worker, exc=None):
        if self.eating:
            self.eating.cancel()

    def worker_info(self, philosopher, data=None):
        '''Override :meth:`~.Application.worker_info` to provide
        information about the philosopher.'''
        data['philosopher'] = {'number': philosopher.number,
                               'eaten': self.eaten}

    async def eat(self, philosopher):
        '''The ``philosopher`` performs one of these two actions:

        * eat, if he has both forks and then :meth:`release_forks`.
        * try to :meth:`pickup_fork`, if he has fewer than 2 forks.
        '''
        loop = philosopher._loop
        while True:
            forks = self.forks
            if forks:
                #
                # Two forks. Eat!
                if len(forks) == 2:
                    self.thinking = 0
                    self.eaten += 1
                    philosopher.logger.info("eating... So far %s times",
                                            self.eaten)
                    eat_time = 2*self.cfg.eating_period*random.random()
                    await sleep(eat_time)
                    await self.release_forks(philosopher)
                #
                # One fork only! release fork or try to pick one up
                elif len(forks) == 1:
                    waiting_period = 2*self.cfg.waiting_period*random.random()
                    if self.started_waiting == 0:
                        self.started_waiting = loop.time()
                    elif loop.time() - self.started_waiting > waiting_period:
                        philosopher.logger.debug("tired of waiting")
                        await self.release_forks(philosopher)
                #
                # this should never happen
                elif len(forks) > 2:    # pragma    nocover
                    philosopher.logger.critical('more than 2 forks!!!')
                    await self.release_forks(philosopher)
            else:
                if not self.thinking:
                    philosopher.logger.warning('thinking...')
                self.thinking += 1
            await self.pickup_fork(philosopher)

    async def release_forks(self, philosopher):
        '''The ``philosopher`` has just eaten and is ready to release both
        forks.

        This method releases them, one by one, by sending the ``put_down``
        action to the monitor.
        '''
        forks = self.forks
        self.forks = []
        self.started_waiting = 0
        for fork in forks:
            philosopher.logger.debug('Putting down fork %s', fork)
            await philosopher.send('monitor', 'putdown_fork', fork)
        await sleep(self.cfg.waiting_period)

    async def pickup_fork(self, philosopher):
        fork = await philosopher.send(philosopher.monitor, 'pickup_fork',
                                      philosopher.number)
        if fork:
            forks = self.forks
            if fork in forks:
                philosopher.logger.error('Got fork %s. I already have it',
                                         fork)
            else:
                philosopher.logger.debug('Got fork %s.', fork)
                forks.append(fork)

    def actorparams(self, monitor, params):
        avail = set(range(1, monitor.cfg.workers+1))
        for philosopher in monitor.managed_actors.values():
            info = philosopher.info
            if info:
                avail.discard(info['philosopher']['number'])
            else:
                avail = None
                break
        number = min(avail) if avail else len(monitor.managed_actors) + 1
        params.update({'name': 'Philosopher %s' % number, 'number': number})


if __name__ == '__main__':      # pragma    nocover
    DiningPhilosophers().start()
