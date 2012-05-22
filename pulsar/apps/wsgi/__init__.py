"""
Pulsar is shipped with an HTTP :class:`pulsar.apps.Application` for
serving web applications which conforms with the python web server
gateway interface (WSGI_).

The application can be used in conjunction with several web frameworks
as well as the :ref:`pulsar RPC middleware <apps-rpc>` and
the :ref:`websocket middleware <apps-ws>`.

An example of a web server written with ``pulsar.apps.wsgi`` which responds 
with "Hello World!" for every request:: 

    from pulsar.apps import wsgi
    
    def hello(environ, start_response):
        data = b"Hello World!"
        response_headers = (
            ('Content-type','text/plain'),
            ('Content-Length', str(len(data)))
        )
        start_response("200 OK", response_headers)
        return [data]
    
    if __name__ == '__main__':
        wsgi.WSGIApplication(callable=hello).start()


For more information regarding WSGI check the pep3333_ specification.

This application uses the WSGI primitive in :mod:`pulsar.net`

.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _WSGI: http://www.wsgi.org
"""
import sys
from inspect import isclass

import pulsar
from pulsar.net import HttpResponse
from pulsar.utils.importer import module_attribute
from pulsar.apps import socket

from .wsgi import *
from . import middleware

class WsgiSetting(pulsar.Setting):
    virtual = True
    app = 'wsgi'
    
class Keepalive(WsgiSetting):
    name = "keepalive"
    flags = ["--keep-alive"]
    validator = pulsar.validate_pos_int
    type = int
    default = 5
    desc = """\
        The number of seconds to wait for requests on a Keep-Alive connection.
        
        Generally set in the 1-5 seconds range.    
        """
        
        
class HttpParser(WsgiSetting):
    name = "http_parser"
    flags = ["--http-parser"]
    desc = """\
        The HTTP Parser to use. By default it uses the fastest possible.    
        
        Specify `python` if you wich to use the pure python implementation    
        """
            
    
class ResponseMiddleware(WsgiSetting):
    name = "response_middleware"
    flags = ["--response-middleware"]
    nargs = '*'
    desc = """\
    Response middleware to add to the wsgi handler    
    """
    
    
class HttpError(WsgiSetting):
    name = "handle_http_error"
    validator = pulsar.validate_callable(2)
    type = "callable"
    default = staticmethod(handle_http_error)
    desc = """\
Render an error occured while serving the WSGI application.

The callable needs to accept two instance variables for the response
and the error instance."""


class WSGIApplication(socket.SocketServer):
    cfg_apps = ('socket',)
    _name = 'wsgi'
    
    def handler(self):
        callable = self.callable
        if getattr(callable,'wsgifactory',False):
            callable = callable()
        return self.wsgi_handler(callable)
    
    def wsgi_handler(self, hnd, resp_middleware = None):
        '''Build the wsgi handler from *hnd*. This function is called
at start-up only.

:parameter hnd: This is the WSGI handle which can be A :class:`WsgiHandler`,
    a WSGI callable or a list WSGI callables.
:parameter resp_middleware: Optional list of response middleware functions.'''
        if not isinstance(hnd, WsgiHandler):
            if not isinstance(hnd, (list,tuple)):
                hnd = [hnd]
            hnd = WsgiHandler(hnd)
        response_middleware = self.cfg.response_middleware or []
        for m in response_middleware:
            if '.' not in m:
                mm = getattr(middleware,m,None)
                if not mm:
                    raise ValueError('Response middleware "{0}" not available'\
                                     .format(m))
            else:
                mm = module_attribute(m)
            if isclass(mm):
                mm = mm()
            hnd.response_middleware.append(mm)
        if resp_middleware:
            hnd.response_middleware.extend(resp_middleware)
        return hnd
        
    def stream_request(self, stream, client_address):
        return HttpRequest(stream, client_address=client_address,
                           timeout=self.worker.cfg.keepalive)
        
    def handle_request(self, worker, request):
        '''handle the *request* by building the WSGI environment '''
        cfg = worker.cfg
        concurrency = self.concurrency(cfg)
        mt = concurrency == 'thread' and cfg.workers > 1
        mp = concurrency == 'process' and cfg.workers > 1
        environ = request.wsgi_environ(multithread=mt,
                                       multiprocess=mp)
        if not environ:
            yield request.on_body
            environ = request.wsgi_environ(actor=worker,
                                           multithread=mt,
                                           multiprocess=mp)
        # Create the response object
        response = HttpResponse(request)
        start_response = response.start_response
        if environ:
            # Get the data from the WSGI handler
            try:
                data = worker.app_handler(environ, start_response)
            except Exception as e:
                # we make sure headers where not sent
                try:
                    start_response('500 Internal Server Error', [],
                                   sys.exc_info())
                except:
                    # The headers were sent already
                    self.log.critical('Headers already sent!',
                                      exc_info=sys.exc_info())
                    data = iter([b'Critical Server Error'])
                else:
                    # Create the error response
                    data = WsgiResponse(environ=environ)
                    cfg.handle_http_error(data, e)
                    data(environ, start_response)
            # delegate the writing of data to the response instance
            yield response.write(data)
        yield response
    


