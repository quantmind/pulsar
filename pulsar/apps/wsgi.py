import os
import sys
import socket

from pulsar import Application, ThreadQueue
from pulsar.utils.importer import import_app
from pulsar.http.utils import write_nonblock, write_error, close


class WSGIApplication(Application):
    
    def init(self, parser, opts, args):
        if self.callable is None:
            parser.error("No application module specified.")

    def load(self):
        return import_app(self.app_uri)
    
    def get_task_queue(self): 
        if self.cfg.concurrency == 'process':
            return None
        else:
            return ThreadQueue()
        
    def handle_event_task(self, worker, req):
        try:
            response, environ = req.wsgi(worker = worker)
            response.force_close()
            return response, self.callable(environ, response.start_response)
        except StopIteration:
            worker.log.debug("Ignored premature client disconnection.")
        except socket.error as e:
            if e[0] != errno.EPIPE:
                worker.log.exception("Error processing request.")
            else:
                worker.log.debug("Ignoring EPIPE")

    def end_event_task(self, worker, response, result):
        try:
            if isinstance(response,Exception):
                raise response
            for item in result:
                response.write(item)
            response.close()
            if hasattr(result, "close"):
                result.close()
        except StopIteration:
            worker.log.debug("Ignored premature client disconnection.")
        except socket.error as e:
            if e[0] != errno.EPIPE:
                worker.log.exception("Error processing request.")
            else:
                worker.log.debug("Ignoring EPIPE")
        except Exception as e:
            worker.log.exception("Error processing request: {0}".format(e))
            if not self.debug:
                mesg = "HTTP/1.1 500 Internal Server Error\r\n\r\n"
                write_nonblock(response.sock, mesg)
            else:
                write_error(response.sock, traceback.format_exc())
        finally:    
            close(response.sock)


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    

