import os
import json

import pulsar

from .git import Git
from .utils import change_version


class ReleaseSetting(pulsar.Setting):
    virtual = True
    app = 'release'
    section = "Release Manager"


class DryRun(ReleaseSetting):
    name = "dry_run"
    flags = ['--dry-run']
    action = "store_true"
    default = False
    desc = "Don't create the tag in github"


class VersionFile(ReleaseSetting):
    name = "version_file"
    flags = ["--version_file"]
    default = "version.py"
    desc = """\
        Python module containing the VERSION = ... line
        """


class ChangeVersion(ReleaseSetting):
    name = "change_version"
    validator = pulsar.validate_callable(2)
    type = "callable"
    default = staticmethod(change_version)
    desc = """\
        Change the version number in the code
        """


class ReleaseManager(pulsar.Application):
    name = 'release'
    cfg = pulsar.Config(apps=['release'])

    def monitor_start(self, monitor, exc=None):
        cfg = self.cfg
        cfg.set('workers', 0)

    def worker_start(self, worker, exc=None):
        try:
            yield from self.release()
        except Exception as exc:
            self.logger.exception(str(exc))
            worker._loop.call_soon(self._exit, 1)
        else:
            worker._loop.call_soon(self._exit, None)

    def _exit(self, exit_code):
        pulsar.arbiter().stop(exit_code=exit_code)

    def release(self):
        git = yield from Git.create(self.cfg)
        path = yield from git.toplevel()
        self.logger.info('Repository directory %s', path)

        with open(os.path.join(path, 'release', 'release.json'), 'r') as file:
            release = json.load(file)

        # Validate new tag and write the new version
        tag_name = release['tag_name']
        version = yield from git.validate_tag(tag_name)
        self.cfg.change_version(self, tuple(version))
        #
        if not self.cfg.dry_run:
            with open(os.path.join(path, 'release', 'notes.md'), 'r') as file:
                release['body'] = file.read()

            #
            self.logger.info('Commit changes')
            result = yield from git.commit(msg='Release %s' % tag_name)
            self.logger.info(result)
            self.logger.info('Push changes changes')
            result = yield from git.push()
            self.logger.info(result)

            self.logger.info('Creating a new tag %s' % tag_name)
            tag = yield from git.create_tag(release)
            self.logger.info('Congratulation, the new release %s is out', tag)
