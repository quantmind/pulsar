#!/usr/bin/env python
import sys
import os
from multiprocessing import current_process


def run(**params):
    args = params.get('argv', sys.argv)
    if '--coverage' in args or params.get('coverage'):
        import coverage
        p = current_process()
        p._coverage = coverage.coverage(data_suffix=True)
        p._coverage.start()
    runtests(**params)


def runtests(cov=None, **params):
    from pulsar.utils.path import Path
    from pulsar.apps.test import TestSuite
    from pulsar.apps.test.plugins import bench, profile
    import pulsar.utils.settings.backend
    #
    path = Path(__file__)
    path.add2python('stdnet', 1, down=['python-stdnet'], must_exist=False)
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
        coveralls(strip_dirs=[path.parent.parent, os.getcwd()],
                  stream=suite.stream,
                  repo_token='CNw6W9flYDDXZYeStmR1FX9F4vo0MKnyX')


if __name__ == '__main__':
    run()
