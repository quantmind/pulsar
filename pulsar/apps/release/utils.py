import os
from datetime import date


def passthrough(manager, version):
    pass


def change_version(manager, version):

    def _generate():
        with open(manager.cfg.version_file, 'r') as file:
            text = file.read()

        for line in text.split('\n'):
            if line.startswith('VERSION = '):
                yield 'VERSION = %s' % str(version)
            else:
                yield line

    text = '\n'.join(_generate())
    with open(manager.cfg.version_file, 'w') as file:
        file.write(text)


def write_notes(manager, path, version, release):
    history = os.path.join(path, 'release', 'history')
    if not os.path.isdir(history):
        return False
    dt = date.today()
    dt = dt.strftime('%Y-%b-%d')
    vv = '.'.join((str(s) for s in version[:2]))
    filename = os.path.join(history, '%s.md' % vv)
    body = ['# Ver. %s - %s' % (release['tag_name'], dt),
            '\n',
            release['body']]

    add_file = True

    if os.path.isfile(filename):
        # We need to add the file
        add_file = False
        with open(filename, 'r') as file:
            body.append('\n')
            body.append(file.read())

    with open(filename, 'w') as file:
        file.write('\n'.join(body))

    manager.logger.info('Added notes to changelog')

    if add_file:
        yield from manager.git.add(filename)
