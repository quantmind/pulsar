import sys
import os

import coverage


if __name__ == '__main__':
    if sys.version_info > (3, 3):
        cov = coverage.coverage(data_suffix='travisrun')
        cov.start()
        from runtests import run, Path
        from pulsar.utils.cov import coveralls
        import pulsar
        suite = run(coverage=True, profile=True)
        stream = pulsar.get_actor().stream
        stream.write('Collecting coverage information\n')
        cov.stop()
        cov.save()
        cov = coverage.coverage(data_suffix=True)
        cov.combine()
        cov.save()
        path = Path(pulsar.__file__)
        coveralls(strip_dirs=[path.parent.parent, os.getcwd()],
                  stream=stream,
                  repo_token='CNw6W9flYDDXZYeStmR1FX9F4vo0MKnyX')

    else:
        from runtests import run
        run()
