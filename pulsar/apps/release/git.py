import os
import asyncio
import logging
import configparser

from pulsar import ImproperlyConfigured
from pulsar.apps.http import HttpClient


class Git:

    @classmethod
    def create(cls, cfg):
        auth = cls.auth()
        remote = yield from cls.execute('git', 'config', '--get',
                                        'remote.origin.url')
        raw = remote.split('@')
        assert len(raw), 2
        raw = raw[1]
        domain, path = raw.split(':')
        assert path.endswith('.git')
        path = path[:-4]
        if domain == 'github.com':
            return Github(cfg, auth, domain, path)
        else:
            return Gitlab(cfg, auth, domain, path)

    @classmethod
    def auth(cls):
        home = os.path.expanduser("~")
        config = os.path.join(home, '.gitconfig')
        if not os.path.isfile(config):
            raise ImproperlyConfigured('.gitconfig file not available in %s'
                                       % home)

        parser = configparser.ConfigParser()
        parser.read(config)
        if 'user' in parser:
            user = parser['user']
            if 'username' not in user:
                raise ImproperlyConfigured('Specify username in %s user '
                                           'section' % config)
            if 'token' not in user:
                raise ImproperlyConfigured('Specify token in %s user section'
                                           % config)
            return (user['username'], user['token'])
        else:
            raise ImproperlyConfigured('No user section in %s' % config)

    @classmethod
    def execute(cls, *args):
        proc = yield from asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT)
        yield from proc.wait()
        data = yield from proc.stdout.read()
        return data.decode('utf-8').strip()

    def __init__(self, cfg, auth, domain, path):
        self.cfg = cfg
        self.auth = auth
        self.domain = domain
        self.repo_path = path
        self.logger = logging.getLogger('pulsar.release')
        self.http = HttpClient(headers=[('Content-Type', 'application/json')])

    @property
    def api_url(self):
        return self.domain

    def __repr__(self):
        return self.api_url
    __str__ = __repr__

    def toplevel(self):
        level = yield from self.execute('git', 'rev-parse', '--show-toplevel')
        return level

    def branch(self):
        name = yield from self.execute('git', 'rev-parse',
                                       '--abbrev-ref', 'HEAD')
        return name

    def add(self, *files):
        if files:
            result = yield from self.execute('git', 'add', *files)
            return result

    def commit(self, *files, msg=None):
        if not files:
            files = ('-a',)
        files = list(files)
        files.append('-m')
        files.append(msg or 'commit from pulsar.release manager')
        result = yield from self.execute('git', 'commit', *files)
        return result

    def push(self):
        name = yield from self.branch()
        result = yield from self.execute('git', 'push', 'origin', name)
        if '[rejected]' in result:
            raise RuntimeError(result)
        return result

    def semantic_version(self, tag):
        '''Get a valid semantic version for tag
        '''
        try:
            version = list(map(int, tag.split('.')))
            assert len(version) == 3
            return tuple(version)
        except Exception:
            raise ImproperlyConfigured('Could not parse tag, please use '
                                       'MAJOR.MINOR.PATCH')

    def validate_tag(self, tag_name):
        raise NotImplementedError

    def create_tag(self, release):
        raise NotImplementedError


class Github(Git):

    @property
    def api_url(self):
        return 'https://api.github.com'

    def latest_release(self, repo_path=None):
        repo_path = repo_path or self.repo_path
        url = '%s/repos/%s/releases/latest' % (self.api_url, repo_path)
        self.logger.info('Check current Github release from %s', url)
        response = yield from self.http.get(url, auth=self.auth)
        if response.status_code == 200:
            data = response.json()
            current = data['tag_name']
            self.logger.info('Current Github release %s created %s by %s',
                             current, data['created_at'],
                             data['author']['login'])
            return self.semantic_version(current)
        elif response.status_code == 404:
            self.logger.warning('No Github releases')
        else:
            response.raise_for_status()

    def validate_tag(self, tag_name):
        new_version = self.semantic_version(tag_name)
        version = list(new_version)
        version.append('final')
        version.append(0)
        current = yield from self.latest_release()
        if current and current >= new_version:
            raise ImproperlyConfigured('Current version is %s' % str(current))
        return tuple(version)

    def create_tag(self, release):
        url = '%s/repos/%s/releases' % (self.api_url, self.repo_path)
        response = yield from self.http.post(url, data=release, auth=self.auth)
        response.raise_for_status()
        result = response.json()
        return result['tag_name']


def Gitlab(Git):

    @property
    def api_url(self):
        return 'https://%s/api/v3' % self.domain
