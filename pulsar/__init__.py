'''Concurrent server and message queues'''

VERSION = (0, 1, 'dev')


def get_version():
    return '.'.join(map(str,VERSION))

SERVER_SOFTWARE = "pulsar/%s" % get_version()

__version__   = get_version()
__license__   = "BSD"
__author__    = "Luca Sbardella"
__contact__   = "luca.sbardella@gmail.com"
__homepage__  = "https://github.com/quantmind/pulsar"
__docformat__ = "restructuredtext"


class PulsarException(Exception):
    '''base class of all Pulsar Exception'''