'''
The dining philosophers problem is an example problem often used in concurrent
algorithm design to illustrate synchronization issues and techniques
for resolving them.

The problem
===================

Five silent philosophers sit at a table around a bowl of spaghetti.
A fork is placed between each pair of adjacent philosophers.


        P  f  P
      f         f
    P      O      P
       f        f
           P
           
Each philosopher `P` must alternately think and eat.
Eating is not limited by the amount of spaghetti left: assume an infinite
supply.

However, a philosopher can only eat while holding both the fork to the left and
the fork to the right.
Each philosopher can pick up an adjacent fork, when available, and put it down,
when holding it. These are separate actions: forks must be picked up and put
down one by one.

This implementation will just work. No starvation or dead-lock.
'''
import pulsar
import random
import time

arbiter = pulsar.arbiter()
lag = 2


def talk(self,msg,wait=False):
    self.log.info(msg)
    if wait:
        try:
            time.sleep(wait)
        except IOError:
            pass


def thinking(self,wait=False):
    talk(self,'Thinking.......... eat {0} in {1} loops...'\
         .format(self._eat,self._nr),wait)


class Philosopher(pulsar.Actor):
        
    def on_init(self, left_fork = None, right_fork = None, **kwargs):
        self.left_fork = left_fork
        self.right_fork = right_fork
        self._nr = 0
        self._eat = 0
        
    def on_task(self):
        self._nr += 1
        try:
            self.left_fork.get(timeout=0.1)
        except pulsar.Empty:
            return thinking(self)
        talk(self,'Got left fork')
        try:
            self.right_fork.get(timeout = 0.5)
        except pulsar.Empty:
            talk(self,'Put down left fork')
            self.left_fork.put(True)
            return thinking(self)
        talk(self,'Got right fork')
        talk(self,'Eating...',lag*random.random())
        self._eat += 1
        talk(self,'Put down left fork')
        self.left_fork.put(True)
        talk(self,'Put down right fork')
        self.right_fork.put(True)
        thinking(self,lag*random.random())
    
    #def configure_logging(self, **kwargs):
    #    pass
    
     
def dining():
    # Create 5 forks queues and spawn 5 philosophers
    forks = []
    for i in range(5):
        f = pulsar.Queue(maxsize = 1) 
        f.put(True)
        forks.append(f)
    forks.append(forks[0])
    for i in range(5):
        arbiter.spawn(Philosopher,
                      pool_timeout = 0.01, # All time spent on `on_task`
                      name = 'philosopher-{0}'.format(i+1),
                      left_fork = forks[i],
                      right_fork = forks[i+1],
                      loglevel = 'info')
    
        
def start():
    arbiter.when_running.add_callback(lambda r : dining())\
                        .add_callback(pulsar.raise_failure)
    arbiter.start()
        

if __name__ == '__main__':
    start()
    