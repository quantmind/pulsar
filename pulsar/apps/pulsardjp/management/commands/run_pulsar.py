from djpcms.apps.management.base import BaseCommand
from djpcms.core.exceptions import CommandError

import pulsar
from pulsar.apps.pulsardjp import DjpCmsApplicationCommand


class Command(BaseCommand):
    option_list = BaseCommand.option_list + pulsar.make_options()
    help = "Starts a fully-functional Web server using pulsar."
    args = '[optional port number]'
    
    def handle(self, callable, addrport=None, *args, **options):
        if args:
            raise CommandError('Usage is run_pulsar %s' % self.args)
        
        if addrport:
            options['bind'] = addrport
        
        DjpCmsApplicationCommand(callable = callable,
                                 parse_console = False,
                                 **options).start()
