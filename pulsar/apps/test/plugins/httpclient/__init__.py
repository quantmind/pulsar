from pulsar.apps import test


from .request import HttpTestClientRequest


class HttpClient(test.Plugin):
    
    def getTest(self, test):
        test.get_client = HttpTestClientRequest