#    ORIGINAL FILE FROM TWISTED twisted.python.runtime
#    Modified and adapted for pulsar
#
# -*- test-case-name: twisted.python.test.test_runtime -*-
# Copyright (c) 2001-2008 Twisted Matrix Laboratories.
# See LICENSE for details.


# System imports
import os
import sys
import time
import imp
import socket


knownPlatforms = {
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
    type = knownPlatforms.get(os.name)
    seconds = staticmethod(_timeFunctions.get(type, time.time))

    def __str__(self):
        return '{0} - {1}'.format(self.type, self.name)

    def __repr__(self):
        return '{0}: {1}'.format(self.__class__.__name__, self)

    def isKnown(self):
        """Do we know about this platform?
        """
        return self.type is not None

    def getType(self):
        """Return ``posix``, ``win`` or ``java``"""
        return self.type

    @property
    def is_posix(self):
        return self.type == 'posix'

    @property
    def isMacOSX(self):
        """Return if we are runnng on Mac OS X."""
        return sys.platform == "darwin"

    @property
    def is_winNT(self):
        """Are we running in Windows NT?"""
        if self.getType() == 'win':
            import _winreg
            try:
                k = _winreg.OpenKeyEx(
                    _winreg.HKEY_LOCAL_MACHINE,
                    r'Software\Microsoft\Windows NT\CurrentVersion')
                _winreg.QueryValueEx(k, 'SystemRoot')
                return True
            except WindowsError:
                return False
        # not windows NT
        return False

    @property
    def is_windows(self):
        return self.getType() == 'win'

    def supportsThreads(self):
        """Can threads be created?
        """
        try:
            return imp.find_module('thread')[0] is None
        except ImportError:
            return False

    @property
    def has_multiProcessSocket(self):
        '''Indicates if support for multiprocess sockets is available.
        '''
        return hasattr(socket, 'fromfd')
