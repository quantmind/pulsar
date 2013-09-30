import sys

import coverage
import coveralls


if __name__ == '__main__':
    if sys.version_info > (3, 3):
        cov = coverage.coverage(data_suffix='travisrun')
        cov.start()
        from runtests import run, Path
        import pulsar
        run(coverage=True, profile=True)
        cov.stop()
        cov.save()
        cov = coverage.coverage(data_suffix=True)
        cov.combine()
        cov.save()
        path = Path(pulsar.__file__)
        coveralls.wear(argv=['--base_dir', path.parent.parent])
    else:
        from runtests import run
        run()
