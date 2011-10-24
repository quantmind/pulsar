import sys
import time
import math
import cProfile as profiler
import pstats

if sys.platform == "win32":
    default_timer = time.clock
else:
    default_timer = time.time
    
import pulsar
from pulsar.utils.py2py3 import range

from pulsar.apps import test


class Repeat(test.TestOption):
    flags = ["--repeat"]
    validator = pulsar.validate_pos_int
    type = int
    default = 10
    desc = '''Default number of repetition when benchmarking.'''


class Bench(test.TestOption):
    flags = ["--bench"]
    action = "store_true"
    default = False
    validator = pulsar.validate_bool
    desc = '''Run benchmarks'''

    
class Profile(test.TestOption):
    flags = ["--profile"]
    action = "store_true"
    default = False
    validator = pulsar.validate_bool
    desc = '''Profile benchmarks using the cProfile'''

        
class BenchTest(test.WrapTest):
    
    def __init__(self, test, number):
        super(BenchTest,self).__init__(test)
        self.number = number
        
    def _call(self):
        testMethod = self.testMethod
        t = 0
        t2 = 0
        for r in range(self.number):
            start = default_timer()
            testMethod()
            dt = default_timer() - start
            t += dt
            t2 += dt*dt
        mean = t/self.number
        std = math.sqrt((t2 - t*mean)/self.number)
        std = round(100*std/mean,2)
        return {'number': self.number,
                'mean': mean,
                'std': '{0} %'.format(std)}
        
        
class ProfileTest(object):
    
    def __init__(self, test, number):
        super(ProfileTest,self).__init__(test)
        self.number = number
        
    def _call(self):
        pass
    

class BenchMark(test.Plugin):
    '''Benchmarking addon for pulsar test suite.'''
    profile = False
    bench = False
    repeat = 1
    
    def configure(self, cfg):
        self.profile = cfg.profile
        self.bench = cfg.bench
        self.repeat = cfg.repeat
        
    def getTest(self, test):
        number = getattr(test,'__number__',self.repeat)
        if self.profile:
            return ProfileTest(test,number)
        elif self.bench:
            return BenchTest(test,number)
    
    def import_module(self, mod, parent):
        b = '__benchmark__'
        bench = getattr(mod,b,getattr(parent,b,False))
        setattr(mod,b,bench)
        if self.bench or self.profile:
            if bench:
                return mod
        else:
            if not bench:
                return mod
    
    def addSuccess(self, test):
        if not self.stream:
            return
        if self.bench:
            stream = self.stream.handler('benchmark')
            result = test.result
            if result:
                result['test'] = test
            stream.writeln(\
'{0[test]} repeated {0[number]} times. Average {0[mean]} Stdev {0[std]}'\
                .format(result))
            stream.flush()
            return True
        elif self.profile:
            pass