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
    #'win': time.clock,
    'win': time.time,
    }

class Platform(object):
    """Gives us information about the platform we're running on"""

    name = os.name
    type = knownPlatforms.get(os.name)
    seconds = staticmethod(_timeFunctions.get(type, time.time))

    def __str__(self):
        return '{0} - {1}'.format(self.type,self.name)
    
    def __repr__(self):
        return '{0}: {1}'.format(self.__class__.__name__,self)
    
    def isKnown(self):
        """Do we know about this platform?"""
        return self.type != None

    def getType(self):
        """Return ``posix``, ``win`` or ``java``"""
        return self.type

    def isMacOSX(self):
        """Return if we are runnng on Mac OS X."""
        return sys.platform == "darwin"

    def isWinNT(self):
        """Are we running in Windows NT?"""
        if self.getType() == 'win':
            import _winreg
            try:
                k=_winreg.OpenKeyEx(_winreg.HKEY_LOCAL_MACHINE,
                                    r'Software\Microsoft\Windows NT\CurrentVersion')
                _winreg.QueryValueEx(k, 'SystemRoot')
                return True
            except WindowsError:
                return False
        # not windows NT
        return False

    def isWindows(self):
        return self.getType() == 'win'

    def isVista(self):
        """
        Check if current platform is Windows Vista or Windows Server 2008.

        @return: C{True} if the current platform has been detected as Vista
        @rtype: C{bool}
        """
        if getattr(sys, "getwindowsversion", None) is not None:
            return sys.getwindowsversion()[0] == 6
        else:
            return False

    def supportsThreads(self):
        """Can threads be created?
        """
        try:
            return imp.find_module('thread')[0] is None
        except ImportError:
            return False

    def multiProcessSocket(self):
        ''':rtype: a boolean indicating if support for multiprocess
 sockets is available.
        '''
        return hasattr(socket,'fromfd')
