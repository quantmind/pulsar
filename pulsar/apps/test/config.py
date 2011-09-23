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

