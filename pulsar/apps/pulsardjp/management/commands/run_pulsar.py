import sys

import djpcms

import pulsar
from pulsar.apps.pulsardjp import DjpCmsApplicationCommand


class Command(djpcms.Command):
    help = "Starts a fully-functional Web server using pulsar."
    
    def run_from_argv(self, sites, command, argv):
        self.execute(sites, argv)
        
    def handle(self, callable, argv):
        DjpCmsApplicationCommand(callable = callable,
                                 argv = argv).start()
