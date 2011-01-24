import socket

from pulsar.http import get_httplib
from pulsar.utils.system import IOpoll, close_on_exec
from pulsar.utils.http import write_nonblock, close


class HttpSync(object):
    SIG_TIMEOUT = 0
    POLL_TIMEOUT = 1
    
    def __init__(self, proc):
        self.proc = proc
        self.cfg = proc.cfg
        self.debug = self.cfg.debug or False
        self.nr = 0
        self.log = proc.log
        self.http = get_httplib(proc.cfg)
        self._iopoll = poll = IOpoll()
        poll.register(proc.LISTENER)
        
    def iohandle(self):
        try:
            for fd,etype in self._iopoll.poll(self.POLL_TIMEOUT):
                if etype == self._iopoll.READ:
                    client, addr = fd.accept()
                    client.setblocking(1)
                    close_on_exec(client)
                    self.handle(client, addr)
        except IOError:
            return
        
    def handle(self, client, addr):
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
            self.log.exception("Error processing request.")
            try:            
                # Last ditch attempt to notify the client of an error.
                mesg = "HTTP/1.1 500 Internal Server Error\r\n\r\n"
                #write_nonblock(client, mesg)
            except:
                pass
        finally:   
            close(client)
        
    def handle_request(self, req, client, addr):
        proc = self.proc
        try:
            self.cfg.pre_request(self, req)
            resp, environ = self.http.create_wsgi(req, client, addr, proc.address, proc.cfg)
            # Force the connection closed until someone shows
            # a buffering proxy that supports Keep-Alive to
            # the backend.
            resp.force_close()
            self.nr += 1
            proc.check_num_requests(self.nr)
            respiter = proc.wsgi(environ, resp.start_response)
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
            util.write_error(client, traceback.format_exc())
            return
        finally:
            try:
                self.cfg.post_request(self, req)
            except:
                pass

        