'''Utility classes for Asynchronous Http body strings.

Asynchronous String
=====================

.. autoclass:: AsyncString
   :members:
   :member-order: bysource
   
Asynchronous Html
=====================

.. autoclass:: Html
   :members:
   :member-order: bysource
   
.. _app-wsgi-html-document:

Html Document
==================

.. autoclass:: HtmlDocument
   :members:
   :member-order: bysource
   
StreamRenderer
==================

.. autoclass:: StreamRenderer
   :members:
   :member-order: bysource
   
'''
import json
from collections import Mapping
from functools import partial

from pulsar import Deferred, multi_async, is_async, maybe_async, is_failure, async
from pulsar.utils.pep import iteritems, is_string, ispy3k
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.html import slugify, INLINE_TAGS, tag_attributes, attr_iter,\
                                csslink, dump_data_value, child_tag
from pulsar.utils.httpurl import remove_double_slash, urljoin

__all__ = ['AsyncString', 'Html', 'Json', 'HtmlDocument', 'html_factory']


class StreamRenderer(Deferred):
    '''A specialised :class:`pulsar.Deferred` returned by the
:meth:`AsyncString.content` method.'''
    def __init__(self, stream, renderer, handle_value=None, **params):
        super(StreamRenderer, self).__init__()
        handle_value = handle_value or self._handle_value
        self._m = multi_async(stream, raise_on_error=True,
                              handle_value=handle_value, **params)
        self._m.add_callback(renderer).add_both(self.callback)

    def _handle_value(self, value):
        '''It makes sure that :class:`Content` is unwond.
Ideally this should not occur since the request object is not available.
Users should always call the content method before.'''
        if isinstance(value, AsyncString):
            return value.content()
        else:
            return value
    
if ispy3k:
    def stream_to_string(stream):
        for value in stream:
            if value is None:
                continue
            elif isinstance(value, bytes):
                yield value.decode('utf-8')
            elif isinstance(value, str):
                yield value
            else:
                yield str(value)

else: #pragma nocover
    def stream_to_string(stream):
        for value in stream:
            if value is None:
                continue
            elif isinstance(value, unicode):
                yield value
            else:
                yield str(value)
    

class AsyncString(object):
    '''Class for asynchronous strings which can be used with
pulsar WSGI servers.'''
    content_type = None
    '''Content type for this :class:`AsyncString`'''
    encoding = None
    '''Charset encoding for this :class:`AsyncString`'''
    
    def __init__(self, *children):
        self._streamed = False
        self._children = []
        self._parent = None
        for child in children:
            self.append(child)
    
    def __repr__(self):
        return self.__class__.__name__()
    
    def __str__(self):
        return self.__repr__()
    
    def content(self, request=None):
        '''Return the :class:`StreamRenderer` for this instance.
This method can be called once only.'''
        return StreamRenderer(self.stream(request), self.to_string)
    
    def stream(self, request):
        '''Return an iterable over strings, that means unicode/str
for python 2, and str for python 3. This method can be called once only
otherwise a RuntimeError occurs.'''
        if self._streamed:
            raise RuntimeError('%s already streamed' % self)
        self._streamed = True
        return self._stream(request)
    
    @async()
    def http_response(self, request):
        '''Return the WSGI iterable. The iterable yields empty bytes untill the
:class:`AsyncString` has its result ready. Once ready it sets its value as the
content of a :class:`WsgiResponse`'''
        body = yield self.content(request)
        response = request.response
        response.content_type = self.content_type
        response.content = body
        response.start()
        yield response
    
    def append(self, child):
        # make sure that child is not in child
        if child is not None:
            if isinstance(child, AsyncString):
                if child._parent:
                    child._parent.remove(child)
                child._parent = self
            self._children.append(child)
            return self
        
    def remove(self, child):
        try:
            self._children.remove(child)
            child._parent = None
        except ValueError:
            pass
    
    def render(self, request=None):
        '''A shortcut function for synchronously rendering a Content.
This is useful during testing.'''
        value = maybe_async(self.content(request))
        if is_failure(value):
            value.raise_all()
        elif is_async(value):
            raise ValueError('Could not render. Asynchronous value')
        else:
            return value
    
    def to_string(self, stream):
        '''Once the stream is ready (no more asynchronous elements) this
functions get called to transform the stream into a string. This method
can be overwritten by derived classes.

:param stream: a collections containing data used to build the string.
:return: a string or bytes
'''
        return ''.join(stream_to_string(stream))
    
    def _stream(self, request):
        '''This method can be re-implemented by subclasses'''
        for child in self._children:
            if isinstance(child, AsyncString):
                for bit in child.stream(request):
                    yield bit
            else:
                yield child
            

class Json(AsyncString):
    '''An :class:`AsyncString` which renders into a json string.'''
    def __init__(self, *children, **params):
        super(Json, self).__init__(*children)
        self.parameters = AttributeDictionary(params)
        
    @property
    def json(self):
        return self.parameters.json or json
        
    @property
    def content_type(self):
        return 'application/json'
        
    def to_string(self, stream):
        if len(stream) == 1:
            return self.json.dumps(stream[0])
        else:
            return self.json.dumps(stream)
        

def html_factory(tag, **defaults):
    '''Returns an :class:`Html` factory function for *tag* and a given
dictionary of *defaults* parameters.'''
    def html_input(**params):
        p = defaults.copy()
        p.update(params)
        return Html(tag, **p)
    return html_input


class Html(AsyncString):
    '''An :class:`AsyncString` for html elements.
The :attr:`AsyncString.content_type` attribute is set to `text/html`.
    
.. attribute:: tag
    
    The tag for this HTML element
    
'''
    def __init__(self, tag, *children, **params):
        self._tag = tag
        self._classes = set()
        self._data = {}
        self._attr = {}
        self._css = {}
        self._setup(**params)
        super(Html, self).__init__(*children)
        
    @property
    def content_type(self):
        return 'text/html'
    
    @property
    def tag(self):
        return self._tag
    
    @property
    def available_attributes(self):
        return tag_attributes(self._tag, self._attr.get('type'))
    
    def __repr__(self):
        if self._tag and self._tag in INLINE_TAGS:
            return '<%s%s/>' % (self._tag, self.flatatt())
        elif self._tag:
            return '<%s%s>' % (self._tag, self.flatatt())
        else:
            return self.__class__.__name__
    
    def append(self, child):
        if child:
            tag = child_tag(self._tag)
            if tag:
                if isinstance(child, Html):
                    if child.tag != tag:
                        child = Html(tag, child)
                elif child.startswith('<%s' % tag):
                    child = Html(tag, child)
        return super(Html, self).append(child)
             
    def _setup(self, cn=None, attr=None, css=None, data=None, type=None,
               **params):
        self.addClass(cn)
        self.data(data)
        self.attr(attr)
        self.css(css)
        attributes = self.available_attributes
        if type and type in attrbutes:
            self.attr('type', type)
            attributes = self.available_attributes
        for name, value in iteritems(params):
            if name in attributes:
                self.attr(name, value)
            else:
                self.data(name, value)
        
    def attr(self, name=None, val=None):
        '''Add the specific attribute to the attribute dictionary
with key ``name`` and value ``value`` and return ``self``.'''
        return self._attrdata(self._attr, name, val)
    
    def data(self, name=None, val=None):
        return self._attrdata(self._data, name, val)
    
    def addClass(self, cn):
        '''Add the specific class names to the class set and return ``self``.'''
        if cn:
            if isinstance(cn,(tuple,list,set,frozenset)):
                add = self.addClass
                for c in cn:
                    add(c)
            else:
                add = self._classes.add
                for cn in cn.split():
                    add(slugify(cn, rtx='-'))
        return self

    def hasClass(self, cn):
        '''``True`` if ``cn`` is a class of self.'''
        return cn in self._classes
    
    def removeClass(self, cn):
        '''Remove classes'''
        if cn:
            ks = self._classes
            for cn in cn.split():
                if cn in ks:
                    ks.remove(cn)
        return self
    
    def flatatt(self, **attr):
        '''Return a string with atributes to add to the tag'''
        cs = ''
        attr = self._attr.copy()
        if self._classes:
            cs = ' '.join(self._classes)
            attr['class'] = cs
        if self._css:
            attr['style'] = ' '.join(('%s:%s;' % (k,v)\
                                       for k,v in self._css.items()))
        for k, v in self._data.items():
            attr['data-%s' % k] = dump_data_value(v)
        if attr:
            return ''.join(attr_iter(attr))
        else:
            return ''

    def css(self, mapping=None):
        '''Update the css dictionary if *mapping* is a dictionary, otherwise
 return the css value at *mapping*.'''
        if mapping is None:
            return self._css
        elif isinstance(mapping, Mapping):
            self._css.update(mapping)
            return self
        else:
            return self._css.get(mapping)
    
    def add_to_html(self, request):
        '''The request holds a reference to the Html document being rendered.
This function can be used to add media files or response headers.
For example::

    request.html_documnet.head.scripts.append('http://...')
    request.response.headers['ETag'] = ...
    
By default it does nothing.
'''
        pass
    
    def _stream(self, request):
        if self._tag and self._tag in INLINE_TAGS:
            yield '<%s%s>' % (self._tag, self.flatatt())
        else:
            if self._tag:
                yield '<%s%s>' % (self._tag, self.flatatt())
            for child in self._children:
                if isinstance(child, AsyncString):
                    for bit in child.stream(request):
                        yield bit
                else:
                    yield child
            if self._tag:
                yield '</%s>' % self._tag
        if request:
            self.add_to_html(request)
    
    def _attrdata(self, cont, name, val):
        if name is not None:
            if val is not None:
                if name in cont and isinstance(val, Mapping):
                    cval = cont[name]
                    if isinstance(cval, Mapping):
                        cval.update(val)
                        val = cval
                cont[name] = val
            elif isinstance(name, Mapping):
                cont.update(name)
            else:
                return cont.get(name)
            return self
        else:
            return cont


class Media(object):
    
    def __init__(self, media_path):
        self.media_path = media_path
        
    def absolute_path(self, path):
        if path.startswith('http://') or path.startswith('https://')\
            or path.startswith('/'):
            return path
        return remove_double_slash(urljoin('/%s/' % self.media_path, path))


class Css(AsyncString, Media):
    
    def __init__(self, links=None, media_path=None):
        super(Css, self).__init__()
        Media.__init__(self, media_path)
        self._children = {}
        self.append(links)
        
    def append(self, value):
        if value:
            if isinstance(value, str):
                value = {'all': [value]}
            for media, values in value.items():
                m = self._children.get(media, [])
                for value in values:
                    if value not in m:
                        if not isinstance(value, (tuple, list)):
                            value = csslink(value, None)
                        else:
                            value = csslink(*value)
                        m.append(value)
                self._children[media] = m

    def _stream(self, request):
        for medium in sorted(self._children):
            paths = self._children[medium]
            medium = '' if medium == 'all' else " media='%s'" % medium
            for path in paths:
                link = "<link href='%s' type='text/css'%s rel='stylesheet'/>\n"\
                        % (self.absolute_path(path.link), medium)
                if path.condition:
                    link = '<!--[if %s]>%s<![endif]-->' % (path.condition, link)
                yield link
    
    
class Js(AsyncString, Media):
        
    def append(self, child):
        if child and is_string(child) and child not in self._children:
            self._children.append(child)
    
    def stream(self, request):
        yield '\n'.join(('<script type="text/javascript" src="%s"></script>'\
                         % self.absolute_path(js) for js in self._children))
        
        
class Head(Html):
    '''Html head tag.
    
.. attribute:: title

    Text in the title tag
    
.. attribute:: meta

    Container of meta tags
'''
    def __init__(self, media_path=None, title=None, js=None,
                 css=None, meta=None, charset=None):
        super(Head, self).__init__('head')
        self.title = title
        self.append(Html(None, meta))
        self.append(Css(css))
        self.append(Js(js))
        self.links.media_path = media_path
        self.scripts.media_path = media_path
        self.add_meta(charset=charset or 'utf-8')
    
    @property
    def meta(self):
        return self._children[0]
    
    @property
    def links(self):
        return self._children[1]
    
    @property
    def scripts(self):
        return self._children[2]
    
    def _stream(self, request):
        if self.title:
            self._children.insert(0, '<title>%s</title>' % self.title)
        return super(Head, self)._stream(request)
        
    def body(self, request):
        return self.js_body.content(request)
    
    def add_meta(self, **kwargs):
        meta = Html('meta', **kwargs)
        self.meta.append(meta)
            
    def add(self, other):
        if isinstance(other, Head):
            self.style.append(other.style)
            self.meta.append(other.meta)
            self.js_head.append(other.js_head)
            self.js_body.append(other.js_body)
        return self

    def __add__(self, other):
        if isinstance(other, Media):
            return Media(media=self).add(other)
        else:
            return self
        
        
class Body(Html):
    
    def __init__(self):
        super(Body, self).__init__('body')
        self.scripts = Js()
    
    def _stream(self, request):
        '''Render the widget. It accept two optional parameters, a http
request object and a dictionary for rendering children with a key.

:parameter request: Optional request object.
'''
        self.append(self.scripts)
        return super(Body, self)._stream(request)
                
                
class HtmlDocument(Html):
    '''HTML5 asynchronous document. An instance of this class can be obtained
via the :attr:`pulsar.apps.wsgi.wrappers.WsgiRequest.html_document` attribute.
    
.. attribute:: head

    The Head part of this :class:`HtmlDocument`

.. attribute:: body

    The Body part of this :class:`HtmlDocument`
    
'''
    def __init__(self, title=None, media_path='/media/', charset=None,
                 **params):
        super(HtmlDocument, self).__init__(None, **params)
        self.head = Head(title=title, media_path=media_path, charset=charset)
        self.body = Body()
    
    def __call__(self, title=None, body=None, media_path=None):
        if title:
            self.head.title = title
        if media_path:
            self.head.scripts.media_path = media_path
            self.head.links.media_path = media_path
            self.body.scripts.media_path = media_path
        self.body.append(body)
        return self
    
    def _stream(self, request):
        raise NotImplementedError
    
    def content(self, request):
        r = super(HtmlDocument, self).content(request)
        return r.add_callback(partial(self._finish, request))
        
    def _stream(self, request):
        for b in self.body.stream(request):
            yield b
    
    def _finish(self, request, body):
        return ''.join(('<!DOCTYPE html>',
                        '<html%s>' % self.flatatt(),
                        self.head.render(request),
                        body,
                        '</html>'))
