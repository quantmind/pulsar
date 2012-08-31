'''
The dining philosophers problem is an example problem often used in concurrent
algorithm design to illustrate synchronization issues and techniques
for resolving them.

The problem
===================

Five silent philosophers sit at a round table with each a bowl of spaghetti.
A fork is placed between each pair of adjacent philosophers.


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
    
To run the example, simply type::

    pulsar manage.py
'''
import random
import time
try:
    import pulsar
except ImportError:
    import sys
    sys.path.append('../../')
    import pulsar
from pulsar.async.commands import pulsar_command
    

class Eating_Period(pulsar.Setting):
    flags = ["--eating-period"]
    validator = pulsar.validate_pos_float
    default = 2
    desc = """The average period of eating for a philosopher."""
    
    
class Waiting_Period(pulsar.Setting):
    flags = ["--waiting-period"]
    validator = pulsar.validate_pos_float
    default = 2
    desc = """The average period of waiting for a missing fork."""
    
################################################################################
##    PULSAR COMMANDS FOR DINING PHILOSOPHERS
philosophers_cmommands = set()

@pulsar_command(commands_set=philosophers_cmommands, ack=False)
def putdown_fork(client, actor, fork):
    self = actor.app
    try:
        self.not_available_forks.remove(fork)
    except KeyError:
        self.log.error('Putting down a fork which was already available')    

@pulsar_command(commands_set=philosophers_cmommands)
def pickup_fork(client, actor, fork_right):
    self = actor.app
    num_philosophers = self.cfg.workers
    fork_left = fork_right - 1
    if fork_left == 0:
        fork_left = num_philosophers 
    for fork in (fork_right, fork_left):
        if fork not in self.not_available_forks:
            self.not_available_forks.add(fork)
            return fork


################################################################################
##    DINING PHILOSOPHERS APP
class DiningPhilosophers(pulsar.Application):
    description = 'Dining philosophers sit at a table around a bowl of '\
                  'spaghetti and waits for available forks.'
    commands_set = philosophers_cmommands
    cfg = {'workers': 5}
    
    def monitor_init(self, monitor):
        self.not_available_forks = set()
        
    def worker_task(self, philosopher):
        # Task performed at each loop in the philosopher I/O loop
        local = philosopher.local
        eaten = local.eaten or 0
        forks = local.forks
        started_waiting = local.started_waiting or 0
        if forks:
            max_eat_period = 2*self.cfg.eating_period
            if len(forks) == 2:
                local.thinking = 0
                eaten += 1
                philosopher.log.info("%s eating... So far %s times",
                                     philosopher.name, eaten)
                try:
                    time.sleep(max_eat_period*random.random())
                except IOError:
                    pass
                local.eaten = eaten
                self.release_forks(philosopher)
            elif len(forks) == 1:
                waiting_period = 2*self.cfg.waiting_period*random.random()
                if started_waiting == 0:
                    local.started_waiting = time.time()
                elif time.time() - started_waiting > waiting_period:
                    self.release_forks(philosopher)
                else:
                    self.check_forks(philosopher)
            elif len(forks) > 2:
                philosopher.log.critical('%s has more than 2 forks!!!',
                                         philosopher.name)
                self.release_forks(philosopher)
        else:
            thinking = local.thinking or 0
            if not thinking:
                philosopher.log.warn('%s thinking...', philosopher.name)
            local.thinking = thinking + 1
            self.check_forks(philosopher)
        
    def check_forks(self, philosopher):
        '''The philosopher has less than two forks. Check if forks are
available.'''
        right_fork = philosopher.local.number
        philosopher.send(philosopher.monitor, 'pickup_fork', right_fork)\
                   .add_callback_args(self.got_fork, philosopher)
    
    def release_forks(self, philosopher):
        forks = philosopher.local.forks
        philosopher.local.forks = []
        philosopher.local.started_waiting = 0
        for fork in forks:
            philosopher.log.debug('Putting down fork %s', fork)
            philosopher.send(philosopher.monitor, 'putdown_fork', fork)
    
    def got_fork(self, fork, philosopher):
        if fork:
            forks = philosopher.local.forks
            if fork in forks:
                philosopher.log.error('Got fork %s which I already have' % fork)
            else:
                philosopher.log.debug('Got fork %s.' % fork)
                forks.append(fork)
    
    def actorparams(self, monitor, params):
        number = len(monitor.MANAGED_ACTORS) + len(monitor.SPAWNING_ACTORS) + 1
        name = 'Philosopher %s' % number
        params.update({'name': name,
                       'number': number,
                       'forks': []})
        return params
    

if __name__ == '__main__':
    DiningPhilosophers().start()