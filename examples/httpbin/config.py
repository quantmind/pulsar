#An example on how to use Server Hooks in a config file 

requests = {}

def pre_request(worker, request):
    pass

def post_request(worker, response):
    environ = response.environ
    connection = response.connection
    user_agent = environ['HTTP_USER_AGENT']
    path = environ['PATH_INFO']
    address = environ['REMOTE_ADDR']
    session = connection.session
    request_in_session = connection.processed
    pass
    
def arbiter_task(actor):
    pass