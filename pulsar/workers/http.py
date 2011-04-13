# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
#
import errno
import os
import socket
import traceback
import time
try:
    import ssl
except:
    ssl = None 

import pulsar
from pulsar.http import get_httplib
from pulsar.utils.eventloop import close_on_exec
from pulsar.utils.http import write_nonblock, write_error, close

from .base import WorkerProcess, updaterequests


class HttpHandler(object):
    
    ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED, errno.EWOULDBLOCK)
    ssl_options = None

    def __init__(self, worker):
        self.worker = worker
        
    def __call__(self, fd, events):
        while True:
            try:
                client, addr = self.worker.socket.accept()
                client.setblocking(1)
                close_on_exec(client)
                self.handle(fd, client, addr)
            except socket.error as e:
                if e[0] not in self.ALLOWED_ERRORS:
                    raise
                else:
                    return
                
            # If our parent changed then we shut down.
            #if self.ppid != self.get_parent_id:
            #    self.log.info("Parent changed, shutting down: %s" % self)
            #    self.ioloop.stop()

    def handle(self, fd, *args):
        self.worker.handle_task(fd, (args,))
        

class HttpMixin(object):
    '''A Mixin class for handling syncronous connection over HTTP.'''
    ssl_options = None

    @updaterequests
    def handle_task(self, fd, req):
        try:
            client, addr = req[0]
            parser = self.http.RequestParser(client)
            req = parser.next()
            self.handle_request(req, client, addr)
        except StopIteration:
            self.log.debug("Ignored premature client disconnection.")
        except socket.error as e:
            if e[0] != errno.EPIPE:
                self.log.exception("Error processing request.")
            else:
                self.log.debug("Ignoring EPIPE")
        except Exception as e:
            self.log.exception("Error processing request: {0}".format(e))
            try:            
                # Last ditch attempt to notify the client of an error.
                mesg = "HTTP/1.1 500 Internal Server Error\r\n\r\n"
                write_nonblock(client, mesg)
            except:
                pass
        finally:    
            close(client)

    def handle_request(self, req, client, addr):
        try:
            debug = self.cfg.debug or False
            self.cfg.pre_request(self, req)
            resp, environ = self.http.create_wsgi(req, client, addr, self.address, self.cfg)
            # Force the connection closed until someone shows
            # a buffering proxy that supports Keep-Alive to
            # the backend.
            resp.force_close()
            respiter = self.handler(environ, resp.start_response)
            for item in respiter:
                resp.write(item)
            resp.close()
            if hasattr(respiter, "close"):
                respiter.close()
        except socket.error:
            raise
        except Exception as e:
            # Only send back traceback in HTTP in debug mode.
            if not self.debug:
                raise
            write_error(client, traceback.format_exc())
            return
        finally:
            try:
                self.cfg.post_request(self, req)
            except:
                pass


class Worker(WorkerProcess,HttpMixin):
    '''A Http worker on a child process'''
    worker_name = 'Worker.HttpProcess'
    
    def _run(self, ioloop = None):
        ioloop = self.ioloop
        handler = HttpHandler(self)
        if ioloop.add_handler(self.socket, handler, ioloop.READ):
            self.socket.setblocking(0)
            self.http = get_httplib(self.cfg)
            ioloop.start()

