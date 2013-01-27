#An example on how to use Server Hooks in a config file 

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
    