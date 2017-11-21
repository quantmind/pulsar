import os
import sys
import time
import socket


known_platforms = {
    'nt': 'win',
    'ce': 'win',
    'posix': 'posix',
    'java': 'java',
    'org.python.modules.os': 'java',
    }

_timeFunctions = {
    # 'win': time.clock,
    'win': time.time,
    }


class Platform:
    """Gives us information about the platform we're running on"""

    name = os.name
    type = known_platforms.get(os.name)
    is_windows = (type == 'win')
    is_macosx = (sys.platform == 'darwin')
    is_posix = (type == 'posix')
    has_multiprocessing_socket = hasattr(socket, 'fromfd')
    seconds = staticmethod(_timeFunctions.get(type, time.time))

    def __repr__(self):
        return '{0} - {1}'.format(self.type, self.name)
    __str__ = __repr__
