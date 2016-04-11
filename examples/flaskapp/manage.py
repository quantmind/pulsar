"""Simple Flask application served by pulsar WSGI server on a pool of threads
"""
from flask import Flask, make_response

from pulsar.apps import wsgi


def FlaskApp():
    app = Flask(__name__)

    @app.errorhandler(404)
    def not_found(e):
        return make_response("404 Page", 404)

    @app.route('/', methods=['GET'])
    def add_org():
        return "Flask Example"

    return app


class Site(wsgi.LazyWsgi):

    def setup(self, environ=None):
        app = FlaskApp()
        return wsgi.WsgiHandler((wsgi.wait_for_body_middleware,
                                 wsgi.middleware_in_executor(app)))


def server(**kwargs):
    return wsgi.WSGIServer(Site(), **kwargs)


if __name__ == '__main__':  # pragma    nocover
    server().start()
