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
from pulsar.utils.eventloop import IOLoop, close_on_exec
from pulsar.utils.http import write_nonblock, write_error, close


class HttpMixin(object):
    '''A Mixin class for handling syncronous connection over HTTP.'''
    ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED, errno.EWOULDBLOCK)
    ssl_options = None

    def _handle_events(self, fd, events):
        while True:
            try:
                client, addr = self.socket.accept()
                client.setblocking(1)
                close_on_exec(client)
                self.handle(client, addr)
            except socket.error as e:
                if e[0] not in self.ALLOWED_ERRORS:
                    raise
                else:
                    return
                
            # If our parent changed then we shut down.
            if self.ppid != self.get_parent_id:
                self.log.info("Parent changed, shutting down: %s" % self)
                self.ioloop.stop()
            
    def _tornadoserver(self):
        if self.ssl_options is not None:
            assert ssl, "Python 2.6+ and OpenSSL required for SSL"
            try:
                connection = ssl.wrap_socket(connection,
                                             server_side=True,
                                             do_handshake_on_connect=False,
                                             **self.ssl_options)
            except ssl.SSLError as err:
                if err.args[0] == ssl.SSL_ERROR_EOF:
                    return connection.close()
                else:
                    raise
            except socket.error as err:
                if err.args[0] == errno.ECONNABORTED:
                    return connection.close()
                else:
                    raise
        try:
            if self.ssl_options is not None:
                stream = iostream.SSLIOStream(connection, io_loop=self.io_loop)
            else:
                stream = iostream.IOStream(connection, io_loop=self.io_loop)
            HTTPConnection(stream, address, self.request_callback,
                           self.no_keep_alive, self.xheaders)
        except:
            logging.error("Error in connection callback", exc_info=True)
        
    def handle(self, *args):
        self.handle_loop_event(*args)
        
    def handle_loop_event(self, client, addr):
        try:
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
            self.nr += 1
            if self.nr >= self.max_requests:
                self.log.info("Autorestarting worker after current request.")
                self.alive = False
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
