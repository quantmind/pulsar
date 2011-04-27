from multiprocessing.queues import Empty

import pulsar
from pulsar import system

    
class IOQueue(system.EpollProxy):
    '''The polling mechanism for a task queue. No select or epoll performed here, simply
return task from the queue if available.
This is an interface for using the same IOLoop class of other workers.'''
    def __init__(self, queue):
        super(IOQueue,self).__init__()
        self._queue = queue
        self._fd = id(queue)
        self._empty = []
    
    def fileno(self):
        return self._fd
    
    def poll(self, timeout = 0):
        try:
            req = self._queue.get(timeout = timeout)
            return {self._fd:req}
        except Empty:
            return self._empty


class TaskMonitor(pulsar.WorkerMonitor):
    
    def actor_addtask(self, task_name, args, kwargs, **kwargs):
        request = self.app.make_request(task_name, args, kwargs, ack=True, **kwargs)
        self.task_queue.put(request)
        return request
        
    def actor_addtask_noack(self, task_name, args, kwargs, **kwargs):
        request = self.app.make_request(task_name, args, kwargs, **kwargs)
        self.task_queue.put(request)
    actor_addtask_noack.ack = False
        

class Worker(pulsar.Worker):
    '''A Task worker on a subprocess'''
    _class_code = 'TaskQueue'

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
    