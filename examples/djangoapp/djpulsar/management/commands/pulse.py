# -*- coding: utf-8 -
import pulsar
from pulsar.apps.wsgi import WSGIServer, WsgiHandler, WsgiRequest, PulsarWsgiResponse

from django.conf import settings
from django import http
from django.core import signals
from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import WSGIHandler
from django.core.handlers import base
from django.utils.encoding import force_str
from django.core.handlers.wsgi import logger, set_script_prefix, STATUS_CODE_TEXT
#from django.core.servers.basehttp import get_internal_wsgi_application


class AdminMedia(pulsar.Setting):
    name = "admin_media_path"
    app = 'wsgi'
    section = "WSGI Servers"
    flags = ["--adminmedia"]
    default = ''
    desc = '''Specifies the directory from which to serve admin media.'''
    

PULSAR_OPTIONS = pulsar.make_optparse_options(
                            apps=('socket', 'wsgi'),
                            exclude=('pythonpath', 'django_settings'))


class DjangoWSGIHandler(WSGIHandler):

    def __call__(self, environ, start_response):
        # Set up middleware if needed. We couldn't do this earlier, because
        # settings weren't available.
        r = WsgiRequest(environ, start_response)
        if self._request_middleware is None:
            with self.initLock:
                try:
                    # Check that middleware is still uninitialised.
                    if self._request_middleware is None:
                        self.load_middleware()
                except:
                    # Unload whatever middleware we got
                    self._request_middleware = None
                    raise

        set_script_prefix(base.get_script_name(environ))
        signals.request_started.send(sender=self.__class__)
        try:
            request = self.request_class(environ)
        except UnicodeDecodeError:
            logger.warning('Bad Request (UnicodeDecodeError)',
                exc_info=sys.exc_info(),
                extra={
                    'status_code': 400,
                }
            )
            response = http.HttpResponseBadRequest()
        else:
            response = self.get_response(request)

        response._handler_class = self.__class__
        
        if isinstance(response, PulsarWsgiResponse):
            return response
        
        try:
            status_text = STATUS_CODE_TEXT[response.status_code]
        except KeyError:
            status_text = 'UNKNOWN STATUS CODE'
        status = '%s %s' % (response.status_code, status_text)
        response_headers = [(str(k), str(v)) for k, v in response.items()]
        for c in response.cookies.values():
            response_headers.append((str('Set-Cookie'), str(c.output(header=''))))
        start_response(force_str(status), response_headers)
        return response


class Command(BaseCommand):
    option_list = BaseCommand.option_list + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = 'pulse --help for usage'

    # Validation is called explicitly each time the server is reloaded.
    requires_model_validation = False

    def handle(self, *args, **options):
        if args:
            raise CommandError('pulse --help for usage')
        admin_media_path = options.pop('admin_media_path', '')
        c = DjangoWSGIHandler()
        WSGIServer(callable=c, cfg=options, parse_console=False).start()
        