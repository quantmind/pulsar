import os
import sys
import logging
import time

import pythoncom
import win32serviceutil
import win32service
import win32event
import win32api
import servicemanager

import pulsar

import multiprocessing

from .winprocess import WINEXE


class ServiceManagerLogHandler(logging.Handler):
    
    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                servicemanager.LogErrorMsg(msg)
            elif record.levelno >= logging.INFO:
                servicemanager.LogiNFOMsg(msg)
        except:
            pass
            

def ctrlHandler(ctrlType):
   return True

class PulsarService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PULSAR_%s' % pulsar.__version__
    _svc_display_name_ = "PULSAR %s server" % pulsar.__version__
    _svc_description_ = "Pulsar asynchronous server"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.running = False

    def SvcStop(self):
        #self.log.info('%s - Received stop signal' % self._svc_name_)
        pulsar.arbiter().stop()
        self.running = False
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        self.setup()
    
    def setup(self):
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
        cdir = None
        if not argv or not argv[0]:
            print('bla')
            main_path = getattr(sys.modules['__main__'], '__file__', None)
            path,script = os.path.split(main_path)                
            sys.argv = [script,'start']
        cls.params = params
        win32api.SetConsoleCtrlHandler(ctrlHandler, True)
        win32serviceutil.HandleCommandLine(cls)
        if cdir:
            os.chdir(cdir)


    
if __name__ == '__main__':
    '''To debug type::

    python winservice.py debug
    '''
    PulsarService.run()
