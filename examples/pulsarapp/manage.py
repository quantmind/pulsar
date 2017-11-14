from pulsar.apps.wsgi import Router
from pulsar.apps.wsgi.handlers import WsgiHandler
from pulsar.apps.wsgi import WSGIServer, WsgiResponse


blueprint = Router('/')


@blueprint.router('sync', methods=['get', 'post'])
def sync_case(request):
    return WsgiResponse(200, 'sync')


@blueprint.router('async', methods=['delete', 'put'])
async def async_cast(request):
    return WsgiResponse(200, 'async')


def server(**kwargs):
    return WSGIServer(callable=WsgiHandler((blueprint, )), **kwargs)


if __name__ == '__main__':  # pragma    nocover
    server().start()
