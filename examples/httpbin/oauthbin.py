from oauthlib import oauth2

from pulsar.apps import wsgi
from pulsar.apps.wsgi import route


class Validator(oauth2.RequestValidator):

    def validate_client_id(self, client_id, request):
        return False


server = oauth2.WebApplicationServer(Validator())


class OAuthBin(wsgi.Router):

    @route('<client_id>', defaults={'client_id': 'client_id'})
    def request_oauth2(selfself, request):
        raise Exception
