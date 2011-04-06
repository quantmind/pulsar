from pulsar.apps import wsgi
from pulsar.internet.arbiter import SyncArbiter


class WSGIApplication(wsgi.WSGIApplication):
    Arbiter = SyncArbiter
    


def app(environ, start_response):
    data = "Hello, World!\n"
    start_response("200 OK", [
        ("Content-Type", "text/plain"),
        ("Content-Length", str(len(data)))
    ])
    return iter([data])



if __name__ == '__main__':
    import sys
    sys.argv += 'wsgi1:app',
    WSGIApplication("%prog [OPTIONS] APP_MODULE").run()