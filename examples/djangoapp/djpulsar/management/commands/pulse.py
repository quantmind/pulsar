# -*- coding: utf-8 -
import pulsar
from pulsar.apps.wsgi import WSGIServer, LazyWsgi, WsgiRequest, PulsarWsgiResponse

from django import http
from django.core import signals
from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import WSGIHandler
from django.core.handlers import base
from django.utils.encoding import force_str
from django.core.handlers.wsgi import logger, set_script_prefix, STATUS_CODE_TEXT
#from django.core.servers.basehttp import get_internal_wsgi_application

PULSAR_OPTIONS = pulsar.make_optparse_options(apps=['socket'])


class DjangoWSGIHandler(WSGIHandler):

    def __init__(self):
        super(WSGIHandler, self).__init__()
        with self.initLock:
            try:
                self.load_middleware()
            except:
                # Unload whatever middleware we got
                self._request_middleware = None
                raise
                
    def __call__(self, environ, start_response):
        r = WsgiRequest(environ, start_response)
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
        
        # Pulsar response return it
        if isinstance(response, PulsarWsgiResponse):
            return response
        
        response._handler_class = self.__class__
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


class Wsgi(LazyWsgi):
    
    def setup(self):
        from django.conf import settings
        return DjangoWSGIHandler()
        
    
class Command(BaseCommand):
    option_list = BaseCommand.option_list + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = 'pulse --help for usage'

    # Validation is called explicitly each time the server is reloaded.
    requires_model_validation = False

    def handle(self, *args, **options):
        if args:
            raise CommandError('pulse --help for usage')
        callable = Wsgi()
        if options.pop('dryrun', False) == True:
            return callable
        callable.setup()
        WSGIServer(callable=callable, cfg=options, parse_console=False,
                   name=self.app_name()).start()
                   
    def app_name(self):
        '''Used by the test suite to run several applications.'''
        actor = pulsar.get_actor()
        name = None
        if actor:
            name = actor.params.django_pulsar_name
        return name or 'pulsar_django'
        