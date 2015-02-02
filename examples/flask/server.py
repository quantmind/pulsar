from pulsar.apps import wsgi
from pulsar.apps.wsgi.middleware import middleware_in_executor, wait_for_body_middleware

import routes

class Site(wsgi.LazyWsgi):
    def setup(self, environ=None):
        return wsgi.WsgiHandler(
            [wait_for_body_middleware,
             middleware_in_executor(routes.app)])

def run():
    wsgi.WSGIServer(
        callable=Site(),
        workers=4,
        thread_workers=20,
        bind=("0.0.0.0" + ":" + "8080")).start()

if __name__ == '__main__':
    run()
