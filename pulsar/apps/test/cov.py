import os
import sys

import coverage
from coverage.report import Reporter

from pulsar import new_event_loop
from pulsar.apps.http import HttpClient
from pulsar.utils.system import json
from pulsar.utils.version import gitrepo


COVERALLS_URL = 'https://coveralls.io/api/v1/jobs'


class CoverallsReporter(Reporter):

    def report(self, morfs):
        self.ret = []
        self.report_files(self._coverall, morfs)
        return self.ret

    def _coverall(self, fr, analysis):
        try:
            with open(fr.filename) as fp:
                source = fp.readlines()
        except IOError:
            if not self.config.ignore_errors:
                raise

        if self.config.strip_dirs:
            filename = fr.filename
            for dir in self.config.strip_dirs:
                if filename.startswith(dir):
                    filename = filename.replace(dir, '').lstrip('/')
                    break
        else:
            filename = fr.relname

        self.ret.append({
            'name': filename,
            'source': ''.join(source).rstrip(),
            'coverage': list(self._coverage_list(source, analysis)),
        })

    def _coverage_list(self, source, analysis):
        for lineno, line in enumerate(source, 1):
            if lineno in analysis.statements:
                yield int(lineno not in analysis.missing)
            else:
                yield None


class Coverage(coverage.Coverage):

    def coveralls(self, morfs=None, strip_dirs=None, **kw):
        self.config.from_args(**kw)
        self.config.strip_dirs = strip_dirs
        reporter = CoverallsReporter(self, self.config)
        return reporter.report(morfs)


def coveralls(http=None, url=None,
              data_file=None, repo_token=None,
              git=None, service_name=None,
              service_job_id=None, strip_dirs=None,
              ignore_errors=False, stream=None):
    '''Send a coverage report to coveralls.io.

    :param http: optional http client
    :param url: optional url to send data to. It defaults to ``coveralls``
        api url.
    :param data_file: optional data file to load coverage data from. By
        default, coverage uses ``.coverage``.
    :param repo_token: required when not submitting from travis.

    https://coveralls.io/docs/api
    '''
    if repo_token is None and os.path.isfile('.coveralls-repo-token'):
        with open('.coveralls-repo-token') as f:
            repo_token = f.read().strip()

    if strip_dirs is None:
        strip_dirs = [os.getcwd()]

    stream = stream or sys.stdout
    coverage = Coverage(data_file=data_file)
    coverage.load()
    if http is None:
        http = HttpClient(loop=new_event_loop())

    if not git:
        try:
            git = gitrepo()
        except Exception:   # pragma    nocover
            pass

    data = {'source_files': coverage.coveralls(strip_dirs=strip_dirs,
                                               ignore_errors=ignore_errors)}

    if git:
        data['git'] = git

    if os.environ.get('TRAVIS'):
        data['service_name'] = service_name or 'travis-ci'
        data['service_job_id'] = os.environ.get('TRAVIS_JOB_ID')
    else:
        assert repo_token, 'Requires repo_token if not submitting from travis'

    if repo_token:
        data['repo_token'] = repo_token
    url = url or COVERALLS_URL
    stream.write('Submitting coverage report to %s\n' % url)
    response = http.post(url, files={'json_file': json.dumps(data)})
    stream.write('Response code: %s\n' % response.status_code)
    try:
        info = response.json()
        code = 0
        if 'error' in info:
            stream.write('An error occured while sending coverage'
                         ' report to coverall.io')
            code = 1
        stream.write('\n%s\n' % info['message'])
    except Exception:
        code = 1
        stream.write('Critical error %s\n' % response.status_code)
    return code
