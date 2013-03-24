# Create your views here.

import csv
import os
import tempfile

from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext

from pulsar.apps import ws, pubsub


def home(request):
    return render_to_response('home.html', {
        'HOST': request.environ['HTTP_HOST']
        }, RequestContext(request))



##    Web Socket Chat handler
class Chat(ws.WS):
    '''The websocket handler (:class:`pulsar.apps.ws.WS`) managing the chat
application.'''
    def on_open(self, request):
        '''When a new websocket connection is established it add connection
to the set of clients listening for messages.'''
        pubsub.add_client(request.cache['websocket'])
        
    def on_message(self, request, msg):
        '''When a new message arrives, it publishes to all listening clients.'''
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                pubsub.publish(msg)
                
                
class middleware:
    
    def __init__(self):
        self._web_socket = ws.WebSocket('/message', Chat())
        
    def process_request(self, request):
        return self._web_socket(request.environ, None)
    