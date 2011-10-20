import pulsar

__all__ = ['TestOption']


class TestOption(pulsar.Setting):
    virtual = True
    app = 'test'
    section = "Test"
    
    
class TestLabels(TestOption):
    name = "labels"
    nargs = '*'
    validator = pulsar.validate_list
    desc = """Optional test labels to run. If not provided\
 all tests are run.
 
To see available labels use the -l option."""


class TestType(TestOption):
    name = "test_type"
    meta = "STRING"
    cli = ["--test-type"]
    validator = pulsar.validate_string
    default = 'regression'
    desc = """\
        The test type.
        Possible choices are: regression, bench and profile.
    """


class TestList(TestOption):
    name = "list_labels"
    meta = "STRING"
    cli = ['-l','--list_labels']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """List all test labels without performing tests."""

