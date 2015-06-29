import os
import sys
import logging
import time

import pythoncom            # noqa
import win32serviceutil
import win32service
import win32event
import win32api
import servicemanager

import pulsar
from pulsar.utils.importer import import_module

import multiprocessing      # noqa


class ServiceManagerLogHandler(logging.Handler):

    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                servicemanager.LogErrorMsg(msg)
            elif record.levelno >= logging.INFO:
                servicemanager.LogiNFOMsg(msg)
        except Exception:
            pass


def ctrlHandler(ctrlType):
    return True


class PulsarService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PULSAR_%s' % pulsar.__version__
    _svc_display_name_ = "PULSAR %s server" % pulsar.__version__
    _svc_description_ = "Pulsar asynchronous server"
    _command_lines = 'pulsar_command_line'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = False

    def SvcStop(self):
        # self.log.info('%s - Received stop signal' % self._svc_name_)
        pulsar.arbiter().stop()
        self.running = False
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        mod = import_module(self._command_lines)
        os.remove(os.path.abspath(mod.__file__))
        self.setup(mod.argv)

    def setup(self, argv):
        raise NotImplementedError

    def main(self):
        arbiter = pulsar.arbiter()
        arbiter.log.info('%s - starting up the service' % self._svc_name_)
        self.running = True
        while self.running:
            time.sleep(1)
        arbiter.log.info('%s - exiting up the service' % self._svc_name_)

    @classmethod
    def run(cls, **params):
        argv = sys.argv
        args = sys.argv[:1]
        if len(sys.argv) > 2:
            args += sys.argv[2:]
        f = open(cls._command_lines+'.py', 'w')
        f.write('argv = {0}'.format(args))
        f.close()
        argv = argv[:2]
        if not argv or not argv[0]:
            main_path = getattr(sys.modules['__main__'], '__file__', None)
            path, script = os.path.split(main_path)
            argv = [script, 'start']
        sys.argv = argv
        cls.params = params
        win32api.SetConsoleCtrlHandler(ctrlHandler, True)
        win32serviceutil.HandleCommandLine(cls)


if __name__ == '__main__':
    '''To debug type::

    python winservice.py debug
    '''
    PulsarService.run()
