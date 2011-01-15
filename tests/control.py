import logging
import os
import sys
from pulsar.test import TestSuiteRunner
from pulsar.utils import importer, filesystem 

logger = logging.getLogger()

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}


class Silence(logging.Handler):
    def emit(self, record):
        pass 


def get_tests(dirpath):
    tests = []
    join  = os.path.join
    loc = os.path.split(dirpath)[1]
    for d in os.listdir(dirpath):
        if os.path.isdir(join(dirpath,d)):
            yield (loc,d)


def import_tests(tags,testdir):
    for loc,app in get_tests(testdir):
        if tags and app not in tags:
            logger.debug("Skipping tests for %s" % app)
            continue
        logger.debug("Try to import tests for %s" % app)
        test_module = '{0}.{1}.tests'.format(loc,app)
            
        try:
            mod = importer.import_module(test_module)
        except ImportError as e:
            logger.debug("Could not import tests for %s: %s" % (test_module,e))
            continue
        
        logger.debug("Adding tests for %s" % app)
        yield mod


def setup_logging(verbosity):
    level = LOGGING_MAP.get(verbosity,None)
    if level is None:
        logger.addHandler(Silence())
    else:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
        
        
def run(tags = None, testtype = None, verbosity = 1, show_list = False, itags = None):
    curdir = filesystem.filedir(__file__)
    if curdir not in sys.path:
        sys.path.insert(0,curdir)
    testtype = testtype or 'regression'
    testdir  = os.path.join(curdir,testtype)
    setup_logging(verbosity)
    modules = import_tests(tags,testdir)
    runner  = TestSuiteRunner(verbosity = verbosity, itags = itags)
    runner.run_tests(modules)
    