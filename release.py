import os

from pulsar.apps.release import ReleaseManager
version_file = os.path.join(os.path.dirname(__file__), 'pulsar', '__init__.py')


if __name__ == '__main__':
    ReleaseManager(config='release.py').start()
