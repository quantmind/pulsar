#An example on how to use Server Hooks in a config file 
from pulsar import Failure
import gc

requests = {}

def _pre_request(worker, request):
    pass

def _post_request(worker, response):
    environ = response.environ
    connection = response.connection
    user_agent = environ['HTTP_USER_AGENT']
    path = environ['PATH_INFO']
    address = environ['REMOTE_ADDR']
    session = connection.session
    request_in_session = connection.processed
    pass
    
    
#def when_ready(worker):
#    worker.event_loop.call_repeatedly(5, check_failures)
    
def check_failures():
    n = 0
    for obj in gc.get_objects():
        if isinstance(obj, Failure):
            n += 1
            rs = gc.get_referrers(obj)
            print('Num referres %d' % len(rs))
            for r in rs:
                print(r)
            del rs
    if n:
        print('Failures %s' % n)