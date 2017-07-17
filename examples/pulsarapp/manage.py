from pulsar.apps.wsgi import Router
from pulsar.apps.wsgi.handlers import WsgiHandler
from pulsar.apps.wsgi import WSGIServer


blueprint = Router('/')


@blueprint.router('sync', methods=['get', 'post'])
def sync_case(request):
    return request.response('sync')


@blueprint.router('async', methods=['delete', 'put'])
def async_cast(request):
    return request.response('async')


def server(**kwargs):
    return WSGIServer(callable=WsgiHandler((blueprint, )), **kwargs)


if __name__ == '__main__':  # pragma    nocover
    server().start()
