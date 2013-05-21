# -*- coding: utf-8 -
import sys

import pulsar
from pulsar.apps.wsgi import WSGIServer, LazyWsgi, WsgiRequest, WsgiResponse

from django import http
from django.core.management.base import BaseCommand, CommandError
from django.utils.encoding import force_str
from django.core.handlers.base import get_script_name, signals
from django.core.handlers.wsgi import logger, set_script_prefix,\
                                       WSGIHandler, STATUS_CODE_TEXT

PULSAR_OPTIONS = pulsar.make_optparse_options(apps=['socket'])


class SkipResponse(Exception):
    
    def __init__(self, response):
        self.response = response
        

class WSGI(WSGIHandler):
            
    def __call__(self, environ, start_response):
        WsgiRequest(environ, start_response)
        return super(WSGI, self).__call__(environ, start_response)


class LWsgi(LazyWsgi):
    
    def setup(self):
        from django.conf import settings
        return WSGI()
        
    
class Command(BaseCommand):
    option_list = BaseCommand.option_list + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = 'pulse --help for usage'

    # Validation is called explicitly each time the server is reloaded.
    requires_model_validation = False

    def handle(self, *args, **options):
        if args:
            raise CommandError('pulse --help for usage')
        callable = LWsgi()
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
        