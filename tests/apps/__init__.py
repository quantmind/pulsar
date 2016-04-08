

def dummy(environ, start_response):
    start_response('200 OK', [])
    yield [b'dummy']
