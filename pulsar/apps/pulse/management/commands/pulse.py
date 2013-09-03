# -*- coding: utf-8 -
import pulsar
from pulsar.apps.wsgi import WSGIServer, LazyWsgi

from django.core.management.base import BaseCommand, CommandError
from django.core.wsgi import get_wsgi_application

PULSAR_OPTIONS = pulsar.make_optparse_options(apps=['socket'])


class Wsgi(LazyWsgi):
    
    def setup(self):
        from django.conf import settings
        return get_wsgi_application()
        
    
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
        