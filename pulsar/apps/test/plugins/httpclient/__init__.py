from pulsar.apps import test


from .request import HttpTestClientRequest


class HttpClient(test.Plugin):
    
    def getTest(self, test):
        wsgi = getattr(test,'wsgi_handler',None)
        if wsgi:
            test.client = HttpTestClientRequest(wsgi())