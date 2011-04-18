#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from pulsar import test


def makeoptions():
    parser = OptionParser()
    parser.add_option("-v", "--verbosity",
                      type = int,
                      action="store",
                      dest="verbosity",
                      default=1,
                      help="Tests verbosity level, one of 0, 1, 2 or 3")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="test_type",
                      default='regression',
                      help="Test type, possible choices are: regression, bench and profile")
    parser.add_option("-l", "--list",
                      action="store_true",
                      dest="show_list",
                      default=False,
                      help="Show the list of available profiling tests")
    parser.add_option("-p", "--proxy",
                      action="store",
                      dest="proxy",
                      default='',
                      help="Set the HTTP_PROXY environment variable")
    return parser



class TestExtractor(object):
    TESTMAPPING = {'regression':'tests','bench':'bench','profile':'profile'}
    def __init__(self, path):
        self.path = path
        
    def testdir(self, testtype):
        return os.path.join(self.path,testtype)
    
    def test_module(self, testtype, loc, app):
        return '{0}.{1}.tests'.format(loc,app)
        
        
class ExampleExtractor(TestExtractor):
    
    def testdir(self, testtype):
        return self.path
    
    def test_module(self, testtype, loc, app):
        name = self.TESTMAPPING[testtype]
        return '{0}.{1}.{2}'.format(loc,app,name)

    
def run():
    '''To perform preprocessing before tests add a cfg.py module'''
    dirs = (('examples',ExampleExtractor),
            ('tests',TestExtractor))
    try:
        import cfg
    except ImportError:
        pass
    options, tags = makeoptions().parse_args()
    p = lambda x : os.path.split(x)[0]
    path = p(os.path.abspath(__file__))
    
    running_tests = []
    for t,c in dirs:
        p = os.path.join(path,t)
        if p not in sys.path:
            sys.path.insert(0, p)
        running_tests.append(c(p))
        
    #if options.proxy:
    #    settings.proxies['http'] = options.proxy
    runner  = test.TestSuiteRunner(tags,
                                   options.test_type,
                                   running_tests,
                                   verbosity = options.verbosity)
    runner.run_tests()


if __name__ == '__main__':
    run()