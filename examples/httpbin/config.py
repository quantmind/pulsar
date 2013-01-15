#An example on how to use Server Hooks in a config file 

def pre_request(worker, request):
    pass

def post_request(worker, request):
    environ = request.environ
    pass
    
def arbiter_task(actor):
    pass