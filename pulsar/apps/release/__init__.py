'''Pulsar app for creating releases. Used by pulsar.
'''
import os
import json
from asyncio import async

import pulsar
from pulsar import ImproperlyConfigured, as_coroutine

from .git import Git
from .utils import passthrough, change_version, write_notes


class ReleaseSetting(pulsar.Setting):
    virtual = True
    app = 'release'
    section = "Release Manager"


class NoteFile(ReleaseSetting):
    name = "note_file"
    flags = ["--note-file"]
    default = 'notes.md'
    desc = """\
        File with release notes inside the relase directory
        """


class Commit(ReleaseSetting):
    name = "commit"
    flags = ['--commit']
    action = "store_true"
    default = False
    desc = "Commit changes"


class BeforeCommit(ReleaseSetting):
    name = "before_commit"
    validator = pulsar.validate_callable(2)
    type = "callable"
    default = staticmethod(passthrough)
    desc = """\
        Callback invoked before committing changes
        """


class WriteNotes(ReleaseSetting):
    name = "write_notes"
    validator = pulsar.validate_callable(4)
    type = "callable"
    default = staticmethod(write_notes)
    desc = """\
        Write release notes
        """


class Push(ReleaseSetting):
    name = "push"
    flags = ['--push']
    action = "store_true"
    default = False
    desc = "Push changes to origin"


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

exclude = set(pulsar.Config().settings)
exclude.difference_update(('config', 'loglevel', 'debug'))


class ReleaseManager(pulsar.Application):
    name = 'release'
    cfg = pulsar.Config(apps=['release'], exclude=exclude)

    def monitor_start(self, monitor, exc=None):
        cfg = self.cfg
        cfg.set('workers', 0)

    def worker_start(self, worker, exc=None):
        if not exc:
            worker._loop.call_soon(async, self._start_release(worker))

    def _start_release(self, worker):
        exit_code = 1
        try:
            yield from self._release()
        except ImproperlyConfigured as exc:
            self.logger.error(str(exc))
        except Exception as exc:
            self.logger.exception(str(exc))
        else:
            exit_code = None
        worker._loop.call_soon(self._exit, exit_code)

    def _exit(self, exit_code):
        pulsar.arbiter().stop(exit_code=exit_code)

    def _release(self):
        self.git = git = yield from Git.create(self.cfg)
        path = yield from git.toplevel()
        self.logger.info('Repository directory %s', path)

        with open(os.path.join(path, 'release', 'release.json'), 'r') as file:
            release = json.load(file)

        # Read the release note file
        note_file = self.cfg.note_file
        with open(os.path.join(path, 'release', note_file), 'r') as file:
            release['body'] = file.read().strip()

        yield from as_coroutine(self.cfg.before_commit(self, release))

        # Validate new tag and write the new version
        tag_name = release['tag_name']
        version = yield from git.validate_tag(tag_name)
        self.logger.info('Bump to version %s', version)
        self.cfg.change_version(self, tuple(version))
        #
        if self.cfg.commit or self.cfg.push:
            #
            # Add release note to the changelog
            yield from as_coroutine(self.cfg.write_notes(self, path,
                                                         version, release))
            self.logger.info('Commit changes')
            result = yield from git.commit(msg='Release %s' % tag_name)
            self.logger.info(result)
            if self.cfg.push:
                self.logger.info('Push changes changes')
                result = yield from git.push()
                self.logger.info(result)

                self.logger.info('Creating a new tag %s' % tag_name)
                tag = yield from git.create_tag(release)
                self.logger.info('Congratulation, the new release %s is out',
                                 tag)
