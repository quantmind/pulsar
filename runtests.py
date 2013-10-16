#!/usr/bin/env python
import sys
import os
from multiprocessing import current_process

try:
    import pep8
except ImportError:
    pep8 = None


def run(**params):
    args = params.get('argv', sys.argv)
    if '--pep8' in args:
        args.remove('--pep8')
        if pep8:
            print('Running pep8.')
            pep8style = pep8.StyleGuide(paths=['pulsar', 'examples'],
                                        config_file='setup.cfg')
            options = pep8style.options
            report = pep8style.check_files()
            if options.statistics:
                report.print_statistics()
            if options.benchmark:
                report.print_benchmark()
            if options.testsuite and not options.quiet:
                report.print_results()
            if report.total_errors:
                if options.count:
                    sys.stderr.write(str(report.total_errors) + '\n')
                sys.exit(1)
            print('OK')
            sys.exit(0)
        else:
            print('pep8 must be installed')
            sys.exit(1)
    if '--coverage' in args or params.get('coverage'):
        import coverage
        p = current_process()
        p._coverage = coverage.coverage(data_suffix=True)
        p._coverage.start()
    runtests(**params)


def runtests(cov=None, **params):
    import pulsar
    from pulsar.utils.path import Path
    from pulsar.apps.test import TestSuite
    from pulsar.apps.test.plugins import bench, profile
    import pulsar.utils.settings.backend
    #
    path = Path(__file__)
    path.add2python('stdnet', 1, down=['python-stdnet'], must_exist=False)
    strip_dirs = [Path(pulsar.__file__).parent.parent, os.getcwd()]
    #
    suite = TestSuite(description='Pulsar Asynchronous test suite',
                      modules=('tests',
                               ('examples', 'tests'),
                               ('examples', 'test_*')),
                      plugins=(bench.BenchMark(),
                               profile.Profile()),
                      pidfile='test.pid',
                      **params).start()
    #
    if suite.cfg.coveralls:
        from pulsar.utils.cov import coveralls
        coveralls(strip_dirs=strip_dirs,
                  stream=suite.stream,
                  repo_token='CNw6W9flYDDXZYeStmR1FX9F4vo0MKnyX')


if __name__ == '__main__':
    run()
