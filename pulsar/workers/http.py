# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
#
import errno
import socket
import traceback
try:
    import ssl
except:
    ssl = None 

import pulsar
from pulsar.http.utils import write_nonblock, write_error, close


class HttpHandler(object):
    '''Handle HTTP requests and delegate the response to the worker'''
    ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                      errno.EWOULDBLOCK, errno.EPIPE)
    ssl_options = None

    def __init__(self, worker):
        self.worker = worker
        
    def __call__(self, fd, events):
        client = None
        try:
            client, addr = self.worker.socket.accept()
            client.setblocking(1)
            req = self.worker.http.Request(client,
                                           addr,
                                           self.worker.address,
                                           self.worker.cfg)
        except socket.error as e:
            close(client)
            if e.errno not in self.ALLOWED_ERRORS:
                raise
            else:
                return
        except StopIteration:
            close(client)
            self.worker.log.debug("Ignored premature client disconnection.")
            return
        
        self.handle(fd, req)

    def handle(self, fd, req):
        self.worker.handle_task(fd, req)
        

class HttpPoolHandler(HttpHandler):
    
    def handle(self, fd, req):
        self.worker.task_queue.put((fd,req))


class Worker(pulsar.Worker):
    '''A Http worker on a child process'''
    _class_code = 'Http'
    ssl_options = None
    
    def on_start(self):
        super(Worker,self).on_start()
        # If the worker is a process and it is listening to a socket
        # Add the socket handler to the event loop
        if self.socket and self.task_queue is None:
            self.socket.setblocking(0)
            handler = HttpHandler(self)
            self.ioloop.add_handler(self.socket, handler, self.ioloop.READ)
    
    @classmethod
    def modify_arbiter_loop(cls, wp):
        '''The arbiter listen for client connections and delegate the handling
to the Thread Pool. This is different from the Http Worker on Processes'''
        wp.socket = pulsar.system.create_socket(wp.cfg.address, log = wp.log)
        if wp.socket and wp.task_queue is not None:
            wp.ioloop.add_handler(wp.socket,
                                  HttpPoolHandler(wp),
                                  wp.ioloop.READ)
            
    @classmethod
    def clean_arbiter_loop(cls, wp):
        if wp.socket and wp.task_queue is not None:
            wp.ioloop.remove_handler(wp.socket)
            
    @classmethod
            
    def _handle_task(self, req):
        try:
            response, environ = req.wsgi(worker = self)
            response.force_close()
            return response, self.handler(environ, response.start_response)
        except StopIteration:
            self.log.debug("Ignored premature client disconnection.")
        except socket.error as e:
            if e[0] != errno.EPIPE:
                self.log.exception("Error processing request.")
            else:
                self.log.debug("Ignoring EPIPE")

    def _end_task(self, response, result):
        try:
            for item in result:
                response.write(item)
            response.close()
            if hasattr(result, "close"):
                result.close()
        except StopIteration:
            self.log.debug("Ignored premature client disconnection.")
        except socket.error as e:
            if e[0] != errno.EPIPE:
                self.log.exception("Error processing request.")
            else:
                self.log.debug("Ignoring EPIPE")
        except Exception as e:
            self.log.exception("Error processing request: {0}".format(e))
            if not self.debug:
                mesg = "HTTP/1.1 500 Internal Server Error\r\n\r\n"
                write_nonblock(response.sock, mesg)
            else:
                write_error(response.sock, traceback.format_exc())
        finally:    
            close(response.sock)
    
