import djpcms
from djpcms import http

from pulsar.apps import wsgi


class LoadWsgi(object):
    wsgifactory = True
    
    def __init__(self, site_factory):
        self.site_factory = site_factory
        
    def __call__(self):
        sites = self.site_factory()
        return http.WSGI(sites)


class Command(djpcms.Command):
    help = "Starts a fully-functional Web server using pulsar."
    
    def run_from_argv(self, site_factory, command, argv):
        #Override so that we use pulsar parser
        self.execute(site_factory, argv)
        
    def handle(self, site_factory, argv):
        wsgi.WSGIApplication(callable = LoadWsgi(site_factory),
                             argv = argv).start()
