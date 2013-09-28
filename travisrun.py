import sys

import coverage
from coveralls import cli


if __name__ == '__main__':
    if sys.version_info > (3, 3):
        cov = coverage.coverage(data_suffix='travisrun')
        cov.start()
        from runtests import run
        run(coverage=True, profile=True)
        cov.stop()
        cov.save()
        cov = coverage.coverage(data_suffix=True)
        cov.combine()
        cov.save()
        cli.main(argv=[])
    else:
        from runtests import run
        run()
