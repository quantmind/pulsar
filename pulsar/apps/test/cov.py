import os
import sys

from coverage.report import Reporter
from coverage import coverage

from pulsar import new_event_loop
from pulsar.apps.http import HttpClient
from pulsar.utils.system import json
from pulsar.utils.version import gitrepo


COVERALLS_URL = 'https://coveralls.io/api/v1/jobs'


class CoverallsReporter(Reporter):

    def report(self, strip_dirs, ignore_errors=False):
        ret = []
        strip_dirs = strip_dirs or []
        for cu in self.code_units:
            try:
                with open(cu.filename) as fp:
                    source = fp.readlines()
            except IOError:
                if not ignore_errors:
                    raise
            analysis = self.coverage._analyze(cu)
            coverage_list = [None for _ in source]
            for lineno, line in enumerate(source):
                if lineno + 1 in analysis.statements:
                    coverage_list[lineno] = int(lineno + 1
                                                not in analysis.missing)
            filename = cu.filename
            for dir in strip_dirs:
                if filename.startswith(dir):
                    filename = filename.replace(dir, '').lstrip('/')
                    break
            ret.append({
                'name': filename,
                'source': ''.join(source).rstrip(),
                'coverage': coverage_list,
            })
        return ret


class Coverage(coverage):

    def coveralls(self, strip_dirs, ignore_errors=False):
        reporter = CoverallsReporter(self, self.config)
        reporter.find_code_units(None)
        return reporter.report(strip_dirs, ignore_errors=ignore_errors)


def coveralls(http=None, url=None, data_file=None, repo_token=None, git=None,
              service_name=None, service_job_id=None, strip_dirs=None,
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

    data = {'source_files': coverage.coveralls(strip_dirs, ignore_errors)}

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
