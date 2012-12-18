import pulsar
from pulsar.utils import events
from pulsar.utils.httpurl import ispy3k
from pulsar.apps import test

try:
    import coverage
except ImportError:
    coverage = None


def stop_coverage(sender, **kwargs):
    cov = pulsar.process_local_data('COVERAGE')
    cov.stop()
    

class Coverage(test.TestPlugin):
    ''':class:`pulsar.apps.test.Plugin` for code coverage.'''
    desc = '''Test coverage with coverage'''
    
    def on_start(self):
        if coverage:
            cov = pulsar.process_local_data('COVERAGE')
            if not cov:
                # START COVERAGE FOR THIS PROCESS
                cov = coverage.coverage()
                pulsar.process_local_data().COVERAGE = cov
                cov.start()
                events.bind('exit', stop_coverage, sender=pulsar.Actor)
                