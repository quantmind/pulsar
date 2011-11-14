import pulsar

__all__ = ['TestOption']


class TestOption(pulsar.Setting):
    virtual = True
    app = 'test'
    section = "Test"
    

class TestVerbosity(TestOption):
    name = 'verbosity'
    flags = ['--verbosity']
    type = int
    default = 1
    desc = """Test verbosity, 0, 1, 2, 3"""
    
    
class TestLabels(TestOption):
    name = "labels"
    nargs = '*'
    validator = pulsar.validate_list
    desc = """Optional test labels to run. If not provided\
 all tests are run.
 
To see available labels use the -l option."""


class TestSize(TestOption):
    name = 'size'
    flags = ['--size']
    #choices = ('tiny','small','normal','big','huge')
    default = 'normal'
    desc = """Optional test size."""
    

class TestList(TestOption):
    name = "list_labels"
    flags = ['-l','--list_labels']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """List all test labels without performing tests."""

