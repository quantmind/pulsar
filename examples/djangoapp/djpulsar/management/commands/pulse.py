# -*- coding: utf-8 -
import pulsar
from pulsar.apps.wsgi import WSGIServer, WsgiHandler, WsgiRequest

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import get_wsgi_application
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
        c = WsgiHandler((self._wsgi, get_wsgi_application()))
        WSGIServer(callable=c, cfg=options, parse_console=False).start()
        
    def _wsgi(self, environ, start_response):
        WsgiRequest(environ, start_response)