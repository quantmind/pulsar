import sys
import time
import math

if sys.platform == "win32":
    default_timer = time.clock
else:
    default_timer = time.time
    
import pulsar
from pulsar.utils.py2py3 import range

from pulsar.apps import test

BENCHMARK_TEMPLATE = '{0[test]}\nRepeated {0[number]} times.\
 Average {0[mean]} secs, Stdev {0[std]}.'

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

        
class BenchTest(test.WrapTest):
    
    def __init__(self, test, number):
        super(BenchTest,self).__init__(test)
        self.number = number
        
    def getSummary(self, number, total_time, total_time2, info):
        mean = total_time/number
        std = math.sqrt((total_time2 - total_time*mean)/number)
        std = round(100*std/mean,2)
        info.update({'number': number,
                     'mean': '%.5f' % mean,
                     'std': '{0} %'.format(std)})
        return info
        
    def _call(self):
        testMethod = self.testMethod
        testStartUp = getattr(self.test,'startUp',lambda : None)
        testGetTime = getattr(self.test,'getTime',lambda dt : dt)
        testGetInfo = getattr(self.test,'getInfo',
                              lambda delta, dt, info_dict : None)
        testGetSummary = getattr(self.test,'getSummary', None)
        t = 0
        t2 = 0
        info = {}
        for r in range(self.number):
            testStartUp()
            start = default_timer()
            testMethod()
            delta = default_timer() - start
            dt = testGetTime(delta)
            testGetInfo(delta, dt, info)
            t += dt
            t2 += dt*dt
        info = self.getSummary(self.number, t, t2, info)
        if testGetSummary:
            return testGetSummary(self.number, t, t2, info)
        else:
            return info


class BenchMark(test.Plugin):
    '''Benchmarking addon for pulsar test suite.'''
    bench = False
    repeat = 1
    
    def configure(self, cfg):
        self.bench = cfg.bench
        self.repeat = cfg.repeat
        
    def getTest(self, test):
        number = getattr(test,'__number__',self.repeat)
        if self.bench:
            return BenchTest(test,number)
    
    def import_module(self, mod, parent):
        b = '__benchmark__'
        bench = getattr(mod,b,getattr(parent,b,False))
        setattr(mod,b,bench)
        if self.bench:
            if bench:
                return mod
        else:
            return mod
    
    def addSuccess(self, test):
        if not self.stream:
            return
        if self.bench:
            stream = self.stream.handler('benchmark')
            result = test.result
            template = getattr(test,'benchmark_template',BENCHMARK_TEMPLATE)
            if result:
                result['test'] = test
            stream.writeln(template.format(result))
            stream.flush()
            return True