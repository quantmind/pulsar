from pulsar.apps import test


from .request import HttpTestClientRequest


class HttpClient(test.Plugin):
    '''A test plugin which creates a fake http client for your
 wsgi handler.'''
    def getTest(self, test):
        wsgi = getattr(test,'wsgi_handler',None)
        if wsgi:
            test.client = HttpTestClientRequest(wsgi)