# -*- coding: utf-8 -
from optparse import make_option

import pulsar
from pulsar.utils.importer import module_attribute
from pulsar.apps.wsgi import (WSGIServer, LazyWsgi, WsgiHandler,
                              wait_for_body_middleware)

from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import get_wsgi_application


PULSAR_OPTIONS = pulsar.make_optparse_options(apps=['socket', 'pulse'],
                                              exclude=['debug'])
pulse_app_name = make_option('--pulse-app-name',
                             dest='pulse-app-name',
                             type='string',
                             default='django_pulsar')

pulse_app_name = make_option('--pubsub-server',
                             dest='pubsub_server',
                             type='string',
                             default='')


class Wsgi(LazyWsgi):

    def setup(self, environ):
        from django.conf import settings
        return WsgiHandler((wait_for_body_middleware,
                            get_wsgi_application()))


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (pulse_app_name,) + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = 'pulse --help for usage'

    # Validation is called explicitly each time the server is reloaded.
    requires_model_validation = False

    def handle(self, *args, **options):
        if args:
            raise CommandError('pulse --help for usage')
        name = options.get('pulse-app-name')
        callable = Wsgi()
        if options.pop('dryrun', False) is True:    # used for testing
            return callable
        callable.setup({})
        from django.conf import settings
        # Allow to specify the server factory dotted path in the settings file
        dotted_path = getattr(settings, 'PULSE_SERVER_FACTORY', None)
        if dotted_path:
            server_factory = module_attribute(dotted_path)
        else:
            server_factory = WSGIServer
        PUBSUB_SERVER = getattr(settings, 'PUBSUB_SERVER', None)
        PUBSUB_SERVER = options.get('pubsub_server') or PUBSUB_SERVER
        server_factory(callable=callable, cfg=options, parse_console=False,
                       name=name, data_server=PUBSUB_SERVER).start()
