import pulsar

    
class TestLabels(pulsar.Setting):
    name = "labels"
    app = 'test'
    nargs = '*'
    section = "Test"
    validator = pulsar.validate_list
    desc = """Optional test labels to run. If not provided\
 all tests are run.
 
To see available labels use the -l option."""


class TestType(pulsar.Setting):
    name = "test_type"
    app = 'test'
    section = "Test"
    meta = "STRING"
    cli = ["--test-type"]
    validator = pulsar.validate_string
    default = 'regression'
    desc = """\
        The test type.
        Possible choices are: regression, bench and profile.
    """


class TestList(pulsar.Setting):
    name = "list_labels"
    app = 'test'
    section = "Test"
    meta = "STRING"
    cli = ['-l','--list_labels']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """List all test labels without performing tests."""


        
class TestLoader(object):
    '''Load test cases'''
    
    def __init__(self, tags, testtype, extractors, itags = None):
        self.tags = tags
        self.testtype = testtype
        self.extractors = extractors
        self.itags = itags
        
    def load(self, suiterunner):
        """Return a suite of all tests cases contained in the given module.
It injects the suiterunner proxy for comunication with the master process."""
        itags = self.itags or []
        tests = []
        for module in self.modules(suiterunner.log):
            for name in dir(module):
                obj = getattr(module, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    tag = getattr(obj,'tag',None)
                    if tag and not tag in itags:
                        continue
                    obj.suiterunner = suiterunner
                    obj.log = suiterunner.log
                    tests.append(obj)
        return self.suiteClass(tests)
    
    def get_tests(self,dirpath):
        join  = os.path.join
        loc = os.path.split(dirpath)[1]
        for d in os.listdir(dirpath):
            if d.startswith('__'):
                continue
            if os.path.isdir(join(dirpath,d)):
                yield (loc,d)
            
    def modules(self, log):
        tags,testtype,extractors = self.tags,self.testtype,self.extractors
        for extractor in extractors:
            testdir = extractor.testdir(testtype)
            for loc,app in self.get_tests(testdir):
                if tags and app not in tags:
                    log.debug("Skipping tests for %s" % app)
                    continue
                log.debug("Try to import tests for %s" % app)
                test_module = extractor.test_module(testtype,loc,app)
                try:
                    mod = import_module(test_module)
                except ImportError as e:
                    log.debug("Could not import tests for %s: %s" % (test_module,e))
                    continue
                
                log.debug("Adding tests for %s" % app)
                yield mod