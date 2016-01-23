import os
from datetime import date

from agile.release import ReleaseManager
version_file = os.path.join(os.path.dirname(__file__), 'pulsar', '__init__.py')

note_file = 'releases/notes.rst'


def write_notes(manager, release):
    filename = 'CHANGELOG.rst'
    dt = date.today()
    dt = dt.strftime('%Y-%b-%d')
    body = ['Ver. %s - %s' % (release['tag_name'], dt),
            '='*28,
            release['body']]

    if os.path.isfile(filename):
        with open(filename, 'r') as file:
            body.append('\n')
            body.append(file.read())

    with open(filename, 'w') as file:
        file.write('\n'.join(body))

    manager.logger.info('Added notes to changelog')


if __name__ == '__main__':
    ReleaseManager(config='release.py').start()
