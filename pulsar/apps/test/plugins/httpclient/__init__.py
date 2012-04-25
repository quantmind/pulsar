from pulsar.apps import test


from .request import HttpTestClientRequest


class HttpClient(test.Plugin):
    '''A test plugin which creates a fake http client for your
 wsgi handler.'''
    def getTest(self, test):
        test.client = lambda : self._get_client(test)
            
    def _get_client(self, test):
        wsgi = getattr(test, 'wsgi_handler', None)
        if wsgi:
            return HttpTestClientRequest(wsgi())