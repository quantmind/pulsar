"""Check PyPI version
"""
from distutils.cmd import Command
from distutils.errors import DistutilsError

try:
    from xmlrpc.client import ServerProxy
except ImportError:
    from xmlrpclib import ServerProxy


class InvalidVersion(DistutilsError):
    pass


class PyPi(Command):
    description = (
        'Validate version with latest PyPI version and, optionally, '
        ' check if version is a valid final release'
    )

    user_options = [
        ('pypi-index-url=', None, 'PyPI index URL'),
        ('final', None,
         'Check if version is a final release (alpha, beta, rc not allowed)')
    ]

    def initialize_options(self):
        self.pypi_index_url = 'https://pypi.python.org/pypi'
        self.final = False

    def finalize_options(self):
        pass

    def run(self):
        version = self.distribution.metadata.version
        if self.final:
            self.check_release(version)

        version = version.split('.')
        current = self.pypi_release().split('.')
        if version <= current:
            raise InvalidVersion(
                'Version %s must be greater then current PyPI version %s'
                % (version, current)
            )

    def pypi_release(self):
        """Get the latest pypi release
        """
        meta = self.distribution.metadata
        pypi = ServerProxy(self.pypi_index_url)
        releases = pypi.package_releases(meta.name)
        if releases:
            return next(iter(sorted(releases, reverse=True)))

    def check_release(self, version):
        try:
            vtuple = list(map(int, version.split('.')))
            assert len(vtuple) == 3
        except Exception:
            raise InvalidVersion(
                "Not a valid final release version %s" % version)
