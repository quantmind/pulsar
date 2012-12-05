import sys
import time
import math

if sys.platform == "win32": #pragma    nocover
    default_timer = time.clock
else:
    default_timer = time.time
    
import pulsar
from pulsar.utils.httpurl import range

from pulsar.apps import test

BENCHMARK_TEMPLATE = '\nRepeated {0[number]} times.\
 Average {0[mean]} secs, Stdev {0[std]}.'

class BenchTest(test.WrapTest):
    __benchmark__ = True
    def __init__(self, test, number):
        super(BenchTest, self).__init__(test)
        self.number = number
        
    def updateSummary(self, info, number, total_time, total_time2):
        mean = total_time/number
        std = math.sqrt((total_time2 - total_time*mean)/number)
        std = round(100*std/mean,2)
        info.update({'number': number,
                     'mean': '%.5f' % mean,
                     'std': '{0} %'.format(std)})
        
    def _call(self):
        simple = lambda info, *args: info
        testMethod = self.testMethod
        testStartUp = getattr(self.test, 'startUp', lambda : None)
        testGetTime = getattr(self.test, 'getTime', lambda dt : dt)
        testGetInfo = getattr(self.test, 'getInfo', simple)
        testGetSummary = getattr(self.test, 'getSummary', simple)
        t = 0
        t2 = 0
        info = {}
        for r in range(self.number):
            testStartUp()
            start = default_timer()
            yield testMethod()
            delta = default_timer() - start
            dt = testGetTime(delta)
            testGetInfo(info, delta, dt)
            t += dt
            t2 += dt*dt
        self.updateSummary(info, self.number, t, t2)
        self.set_test_attribute('bench_info',
                                testGetSummary(info, self.number, t, t2))


class BenchMark(test.TestPlugin):
    '''Benchmarking addon for pulsar test suite.'''
    desc = '''Run benchmarks function flagged with __benchmark__ attribute'''
    
    repeat = pulsar.Setting(type=int,
                            default=1,
                            validator=pulsar.validate_pos_int,
                            desc='Default number of repetition '\
                                 'when benchmarking.''')
        
    def before_test_function_run(self, test, local):
        if self.config.benchmark: 
            method_name = getattr(test, '_testMethodName', None)
            if method_name:
                method = getattr(test, method_name, None)
                bench = getattr(test, '__benchmark__', False)
                if not bench and method:
                    bench = getattr(method, '__benchmark__', False)
                if bench:
                    number = getattr(test, '__number__', self.config.repeat)
                    return BenchTest(test, number)
    
    def addSuccess(self, test):
        if self.config.benchmark and self.stream:
            result = getattr(test, 'bench_info', None)
            if result and self.stream.showAll:
                stream = self.stream.handler('benchmark')
                template = getattr(test, 'benchmark_template',
                                   BENCHMARK_TEMPLATE)
                stream.writeln(template.format(result))
                stream.flush()
                return True
