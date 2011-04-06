import os
import pythoncom
import win32serviceutil
import win32service
import win32event
import win32api
import servicemanager


class SiroService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'SIROWEB_%s' % siro.__version__
    _svc_display_name_ = "SIRO %s WEB server" % siro.__version__
    _svc_description_ = "Web client for Scenario Analysis of Interest Rate Options"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.isAlive = True
        servicemanager.LogInfoMsg('%s - setting up environment' % self._svc_name_)

    def SvcStop(self):
        servicemanager.LogInfoMsg('%s - Received stop signal' % self._svc_name_)
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.isAlive = False
        from unuk.contrib.txweb import stop
        stop()
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg('%s - setting up the server' % self._svc_name_)
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        from siro.siroserv import get_webserver
        self.server = get_webserver()
        self.server.serve()
        
    def main(self):
        import time
        while self.isAlive:
            time.sleep(1)
            

def ctrlHandler(ctrlType):
   return True


def run(): 
    win32api.SetConsoleCtrlHandler(ctrlHandler, True)
    win32serviceutil.HandleCommandLine(SiroService)
    
    
if __name__ == '__main__':
    '''To debug type::

    python winservice.py debug
    '''
    run()
