'''
The dining philosophers problem is an example problem often used in concurrent
algorithm design to illustrate synchronization issues and techniques
for resolving them.

Five silent philosophers sit at a table around a bowl of spaghetti.
A fork is placed between each pair of adjacent philosophers.
Each philosopher must alternately think and eat.
Eating is not limited by the amount of spaghetti left: assume an infinite
supply.

However, a philosopher can only eat while holding both the fork to the left and
the fork to the right.
Each philosopher can pick up an adjacent fork, when available, and put it down,
when holding it. These are separate actions: forks must be picked up and put
down one by one.
'''
import pulsar
import random
import time

lag = 2

def talk(self,msg,wait=False):
    self.log.info(msg)
    if wait:
        time.sleep(wait)

def thinking(self,wait=False):
    talk(self,'Thinking... Done {0} loops...'.format(self.nr),wait)

class Philosopher(pulsar.Actor):
        
    def on_init(self, left_fork = None, right_fork = None, index = None,
                **kwargs):
        self.left_fork = left_fork
        self.right_fork = right_fork
        self.index = index
        self.nr = 0
        
    def on_task(self):
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
        talk(self,'Put down left fork')
        self.left_fork.put(True)
        talk(self,'Put down right fork')
        self.right_fork.put(True)
        self.nr += 1
        thinking(self,lag*random.random())
    
    def configure_logging(self, **kwargs):
        pass
    
    
class Dininig(pulsar.Monitor):
    
    def actorparams(self):
        if not hasattr(self,'forks'):
            self.forks = forks = []
            for i in range(5):
                f = pulsar.Queue(maxsize = 1) 
                f.put(True)
                forks.append(f)
            forks.append(forks[0])
            self.index = 0
        else:
            self.index += 1
        return {'name': 'philosopher-{0}'.format(self.index+1),
                'index': self.index + 1,
                'left_fork': self.forks[self.index],
                'right_fork': self.forks[self.index+1]}
        
        
def start():
    arbiter = pulsar.arbiter()
    arbiter.add_monitor(Dininig, 'dining',
                        num_actors = 5,
                        actor_class = Philosopher,
                        timeout = 0,
                        loglevel = 'info')
    arbiter.start()
        

if __name__ == '__main__':
    print('Setting up')
    start()
    