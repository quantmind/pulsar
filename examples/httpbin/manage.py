'''Pulsar "Hello World!" application. It runs a HTTP server which
display the two famous words::

    python manage.py
    
To see options type::

    python manage.py -h
'''
import re
import os
import json
try:
    import pulsar
except ImportError:
    import sys
    sys.path.append('../../')
    
from pulsar.apps import wsgi, ws
from pulsar.utils.httpurl import Headers, parse_qs, ENCODE_URL_METHODS,\
                                 responses, has_empty_content
from pulsar.utils.multipart import parse_form_data
from pulsar.utils import event

error_messages = {404: '<p>The requested URL was not found on the server.</p>'}

def jsonbytes(data):
    return json.dumps(data, indent=4).encode('utf-8')
    
def getheaders(environ):
    headers = Headers(kind='client')
    for k in environ:
        if k.startswith('HTTP_'):
            headers[k[5:].replace('_','-')] = environ[k]
    return dict(headers)
    
def info_data(environ, **params):
    method = environ['REQUEST_METHOD']
    headers = getheaders(environ)
    args = {}
    if method in ENCODE_URL_METHODS:
        qs = environ.get('QUERY_STRING',{})
        if qs:
            args = parse_qs(qs)
    else:
        post, files = parse_form_data(environ)
        args = dict(post)
    data = {'method': method, 'headers': headers, 'args': args}
    data.update(params)
    return jsonbytes(data)
    
    
class HttpException(Exception):
    status = 500
    def __init__(self, status=500):
        self.status = status
        

class check_method(object):
    
    def __init__(self, method):
        self.method = method
        
    def __call__(self, f):
        def _(obj, environ, *args, **kwargs):
            if self.method != environ['REQUEST_METHOD']:
                raise HttpException(405)
            else:
                return f(obj, environ, *args, **kwargs)
        return _
    

class HttpBin(object):
    
    def __call__(self, environ, start_response):
        '''Pulsar HTTP "Hello World!" application'''
        try:
            path = environ.get('PATH_INFO')
            if path == '/':
                return self.home(environ, start_response)
            elif '//' in path:
                path = re.sub('/+', '/', path)
                return self.redirect(path)
            else:
                path = path[1:]
                bits = [bit for bit in path.split('/')]
                name = bits[0].replace('-','_')
                method = getattr(self, 'request_%s' % name, None)
                if method:
                    bits.pop(0)
                    return method(environ, start_response, bits)
                else:
                    raise HttpException(404)
        except HttpException as e:
            status = e.status
            if has_empty_content(status, environ['REQUEST_METHOD']):
                return wsgi.WsgiResponse(status)
            else:
                content = error_messages.get(status,'')
                title = responses.get(status)
                content = '<h1>%s - %s</h1>%s' % (status, title, content)
                return self.render(content, title=title, status=status)
    
    def redirect(self, location='/'):
        return wsgi.WsgiResponse(302,
                                 response_headers = (('location',location),))
        
    def response(self, data, status=200, content_type=None, headers=None):
        content_type = content_type or 'application/json'
        return wsgi.WsgiResponse(status, content=data,
                                 content_type=content_type)
    
    def load(self, name):
        name = os.path.join(os.path.dirname(os.path.abspath(__file__)),name)
        with open(name,'r') as file:
            return file.read()
                                 
    def render(self, body, title='Pulsar Http', status=200):
        template = self.load('template.html')
        html = (template.format({'body': body, 'title': title})).encode('utf-8')
        return self.response(html, status=status, content_type='text/html')
        
    def home(self, environ, start_response):
        return self.render(self.load('home.html'))
    
    @check_method('GET')
    def request_get(self, environ, start_response, bits):
        if bits:
            raise HttpException(404)
        return self.response(info_data(environ))
    
    @check_method('POST')
    def request_post(self, environ, start_response, bits):
        if bits:
            raise HttpException(404)
        return self.response(info_data(environ))
    
    @check_method('PUT')
    def request_put(self, environ, start_response, bits):
        if bits:
            raise HttpException(404)
        return self.response(info_data(environ))
    
    @check_method('GET')
    def request_redirect(self, environ, start_response, bits):
        if bits:
            if len(bits) > 2:
                raise HttpException(404)
            num = int(bits[0])
        else:
            num = 1
        num -= 1
        if num > 0:
            return self.redirect('/redirect/%s'%num)
        else:
            return self.redirect('/get')
    
    @check_method('GET')
    def request_gzip(self, environ, start_response, bits):
        if bits:
            raise HttpException(404)
        data = self.response(info_data(environ, gzipped=True))
        middleware = wsgi.middleware.GZipMiddleware(10)
        middleware(environ, start_response, data)
        return data
        
    @check_method('GET')
    def request_status(self, environ, start_response, bits):
        number = int(bits[0]) if len(bits) == 1 else 404
        raise HttpException(number)

    @check_method('GET')
    def request_response_headers(self, environ, start_response, bits):
        if bits:
            raise HttpException(404)
        
        class Gen:
            headers=  None
            def __call__(self, value, sender):
                self.headers = value
            def generate(self):
                #yeidl a byte so that headers are sent
                yield '{'
                # we must have the headers now
                headers = jsonbytes(dict(self.headers))
                yield headers[1:]
                
        gen = Gen()
        event.bind('http-headers', gen, once_only=True)            
        data = wsgi.WsgiResponse(
                            200,
                            content=gen.generate(),
                            content_type='application/json',
                            start_response=start_response)
        return data

class handle(ws.WS):
    
    def on_message(self, msg):
        path = self.path
        if path == '/echo':
            return msg
        elif path == '/streaming':
            return json.dumps([(i,random()) for i in range(100)])
        
        
def server(description = None, **kwargs):
    description = description or 'Pulsar HttpBin'
    app = wsgi.WsgiHandler(middleware=(HttpBin(),))
    return wsgi.WSGIApplication(app, description=description, **kwargs)
    

if __name__ == '__main__':
    server().start()
    