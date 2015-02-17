import logging

from pulsar.apps import wsgi
from pulsar.apps.wsgi.middleware import (middleware_in_executor,
                                         wait_for_body_middleware)

import routes


def post_request(response, exc=None):
    '''Handle a post request
    '''
    if not exc:
        logger = logging.getLogger('routes')
        parser = response.parser
        logger.info('%s %s HTTP/%s.%s - %s',
                    parser.get_method(), parser.get_url(),
                    response.version[0], response.version[1], response.status)


class Site(wsgi.LazyWsgi):

    def setup(self, environ=None):
        # setup Flask logger
        app = routes.app()
        return wsgi.WsgiHandler(
            [wait_for_body_middleware,
             middleware_in_executor(app)])


def run():
    wsgi.WSGIServer(Site(), config='manage.py').start()


if __name__ == '__main__':
    run()
