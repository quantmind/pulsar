# -*- coding: utf-8 -
from optparse import make_option

import pulsar
from pulsar.utils.security import random_string
from pulsar.apps.wsgi import (WSGIServer, LazyWsgi, WsgiHandler,
                              wait_for_body_middleware)

from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import get_wsgi_application


PULSAR_OPTIONS = pulsar.make_optparse_options(apps=['socket', 'pulse'])
pulse_app_name = make_option('--pulse-app-name',
                             dest='pulse-app-name',
                             type='string',
                             default='django_pulsar')


class Wsgi(LazyWsgi):

    def setup(self):
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
        callable.setup()
        WSGIServer(callable=callable, cfg=options, parse_console=False,
                   name=name).start()
