'''The :mod:`pulsar.apps.wsgi.content` introduces several utility classes
handling asynchronous content on a WSGI server. The main class of this module
is the :class:`AsyncString`. Web framework are smart ways of concatenating 
bytes which need to be displayed on a web page or returned as a response to
and HTTP client.

Asynchronous String
=====================

.. autoclass:: AsyncString
   :members:
   :member-order: bysource

Asynchronous Json
=====================

.. autoclass:: Json
   :members:
   :member-order: bysource
   
.. _wsgi-html:
   
Asynchronous Html
=====================

.. autoclass:: Html
   :members:
   :member-order: bysource
   
.. _wsgi-html-document:

Html Document
==================

Document
~~~~~~~~~~

.. autoclass:: HtmlDocument
   :members:
   :member-order: bysource
   
Head
~~~~~~~~~~

.. autoclass:: Head
   :members:
   :member-order: bysource
   
Body
~~~~~~~~~~

.. autoclass:: Body
   :members:
   :member-order: bysource
   
StreamRenderer
==================

.. autoclass:: StreamRenderer
   :members:
   :member-order: bysource
   
Html Factory
=================

.. autofunction:: html_factory


'''
import json
from collections import Mapping
from functools import partial
from copy import copy

from pulsar import Deferred, multi_async, is_async, maybe_async, is_failure, async
from pulsar.utils.pep import iteritems, is_string, ispy3k
from pulsar.utils.structures import AttributeDictionary, recursive_update,\
                                    OrderedDict
from pulsar.utils.html import slugify, INLINE_TAGS, tag_attributes, attr_iter,\
                                csslink, dump_data_value, child_tag
from pulsar.utils.httpurl import remove_double_slash, urljoin

from .html import html_visitor

__all__ = ['AsyncString', 'Html',
           'Json', 'HtmlDocument',
           'html_factory', 'Media']


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
pulsar WSGI servers.
'''
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
    
    @property
    def parent(self):
        '''The :class:`AsyncString` element which contains this
:class:`AsyncString`.'''
        return self._parent
    
    @property
    def children(self):
        '''A copy of all children of this :class:`AsyncString`. Children can
be other :class:`AsyncString` or string or bytes, depending on implementation.
:attr:`children` are added and removed via the :meth:`append` and
:meth:`remove` methods.'''
        return copy(self._children)
    
    def __repr__(self):
        return self.__class__.__name__()
    
    def __str__(self):
        return self.__repr__()
    
    def append(self, child):
        '''Append ``child`` to the list of :attr:`children` of this
:class:`AsyncString`.

:param child: String, bytes or another :class:`AsyncString`. If it is an
    :class:`AsyncString`, this instance will be set as its :attr:`parent`.
    If ``child`` is ``None``, this method does nothing.
    
'''
        # make sure that child is not in child
        if child is not None:
            if isinstance(child, AsyncString):
                child_parent = child._parent
                if self._parent is child:
                    # the parent is the child we are appending, set the parent
                    # to be the parent of child
                    self._parent = child_parent
                if child_parent:
                    # remove child from the child parent
                    child_parent.remove(child)
                child._parent = self
            self._children.append(child)
        
    def remove(self, child):
        '''Remove a ``child`` from the list of :attr:``children``.'''
        try:
            self._children.remove(child)
            if isinstance(child, AsyncString):
                child._parent = None
        except ValueError:
            pass
    
    def remove_all(self):
        '''Remove all :attr:`children`.'''
        for child in self._children:
            if isinstance(child, AsyncString):
                child._parent = None
        self._children = []
        
    def append_to(self, parent):
        '''Append itself to ``parent``. Return ``self``.'''
        parent.append(self)
        return self
    
    def content(self, request=None):
        '''Return the :class:`StreamRenderer` for this instance.
This method can be called once only since it invokes the :meth:`stream`
method.'''
        res = self.stream(request)
        if is_async(res):
            return res.add_callback(lambda r: StreamRenderer(r, self.to_string))
        else:
            return StreamRenderer(res, self.to_string)
    
    def stream(self, request):
        '''This is the most important method of an :class:`AsyncString`.
It is called by :meth:`content` or by the :attr:`parent` of this
:class:`AsyncString`. It returns an iterable (list, tuple, generator) over
strings, that means ``unicode/str`` for python 2, and ``str`` for python 3 or
:ref:`asynchronous elements <tutorials-coroutine>` which result in strings.
This method can be called once only, otherwise a
:class:`RuntimeError` occurs.'''
        if self._streamed:
            raise RuntimeError('%s already streamed' % self)
        if request:
            self.stream_started(request)
        self._streamed = True
        return self.do_stream(request)
    
    def stream_started(self, request):
        '''Hook called if ``request`` is available, just before the
:meth:`do_stream` is executed.'''
        pass
    
    @async()
    def http_response(self, request):
        '''Return a, possibly, :ref:`asynchronous WSGI iterable <wsgi-async>`.
This method asynchronously wait for :meth:`content` and subsequently
starts the wsgi response.'''
        response = request.response
        response.content_type = self.content_type
        body = yield self.content(request)
        response.content = body
        yield response
        
    def do_stream(self, request):
        '''Perform the actual streaming of this :class:`AsyncString`.
This method can be re-implemented by subclasses.'''
        for child in self._children:
            if isinstance(child, AsyncString):
                for bit in child.stream(request):
                    yield bit
            else:
                yield child
                
    def to_string(self, stream):
        '''Once the :class:`StreamRenderer`, returned by :meth:`content`
method, is ready, meaning it has no more
asynchronous elements, this method get called to transform the stream into the
content string. This method can be overwritten by derived classes.

:param stream: a collections containing ``strings/bytes`` used to build the
    final ``string/bytes``.
:return: a string or bytes
'''
        return ''.join(stream_to_string(stream))
    
    def render(self, request=None):
        '''A shortcut function for synchronously rendering a Content.
This is useful during testing. It is the synchronous equivalent of
:meth:`content`.'''
        value = maybe_async(self.content(request))
        if is_failure(value):
            value.raise_all()
        elif is_async(value):
            raise ValueError('Could not render. Asynchronous value')
        else:
            return value
            

class Json(AsyncString):
    '''An :class:`AsyncString` which renders into a json string.
The :attr:`AsyncString.content_type` attribute is set to
``application/json``.'''
    def __init__(self, *children, **params):
        super(Json, self).__init__(*children)
        self.parameters = AttributeDictionary(params)
        
    @property
    def json(self):
        '''The ``json`` encoder/decoder handler. If a ``json`` entry is not
provided during initialisation, the standard python ``json`` module
is used.'''
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
    '''Returns an :class:`Html` factory function for ``tag`` and a given
dictionary of ``defaults`` parameters. For example::

    >>> input_factory = html_factory('input', type='text')
    >>> html = input_factory(value='bla')
    
    '''
    def html_input(*children, **params):
        p = defaults.copy()
        p.update(params)
        return Html(tag, *children, **p)
    return html_input
    
    
class Html(AsyncString):
    '''An :class:`AsyncString` for html elements.
The :attr:`AsyncString.content_type` attribute is set to ``text/html``.

:param tag: Set the :attr:`tag` attribute. Must be given and can be ``None``.
:param children: Optional children which will be added via the
    :meth:`AsyncString.append` method.
:param params: Optional keyed-value parameters.

Special (optional) parameters:

* ``cn`` class name or list of class names.
* ``attr`` dictionary of attributes to add.
* ``data`` dictionary of data to add (rendered as HTML data).
* ``type`` type of element, only supported for tags which accept the ``type``
  attribute (for example the ``input`` tag).

Any other keyed-value parameter will be added as attribute, if in the set of
:attr:`available_attributes` or as :meth:`data`.
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
    def classes(self):
        '''Set of classes for this :class:`Html` element.'''
        return self._classes
    
    @property
    def tag(self):
        '''The tag for this HTML element, ``div``, ``a``, ``table`` and so
forth. It can be ``None``.'''
        return self._tag
    
    @property
    def available_attributes(self):
        '''The list of valid HTML attributes for this :attr:`tag`.'''
        return tag_attributes(self._tag, self._attr.get('type'))
    
    def get_form_value(self):
        '''Return the value of this :class:`Html` element when it is contained
in a Html form element. For most element it gets the ``value`` attribute.'''
        return self._visitor.get_form_value(self)
    
    def set_form_value(self, value):
        '''Set the value of this :class:`Html` element when it is contained
in a Html form element. For most element it sets the ``value`` attribute.'''
        self._visitor.set_form_value(self, value)
        
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
                elif not child.startswith('<%s' % tag):
                    child = Html(tag, child)
        super(Html, self).append(child)
    
    def _setup(self, cn=None, attr=None, css=None, data=None, type=None,
               **params):
        self._visitor = html_visitor(self._tag)
        self.addClass(cn)
        self.data(data)
        self.attr(attr)
        self.css(css)
        attributes = self.available_attributes
        if type and 'type' in attributes:
            self.attr('type', type)
            attributes = self.available_attributes
        for name, value in iteritems(params):
            if name in attributes:
                self.attr(name, value)
            else:
                self.data(name, value)
        
    def attr(self, *args):
        '''Add the specific attribute to the attribute dictionary
with key ``name`` and value ``value`` and return ``self``.'''
        if not args:
            return self._attr
        result, adding = self._attrdata(self._attr, *args)
        if adding:
            available_attributes = self.available_attributes
            for name, value in iteritems(result):
                if value is not None:
                    if name in available_attributes:
                        self._attr[name] = value
                    elif name is 'value':
                        self.append(value)
            result = self
        return result
    
    def data(self, *args):
        '''Add or retrieve data values for this :class:`Html`.'''
        if not args:
            return self._data
        result, adding = self._attrdata(self._data, *args)
        if adding:
            recursive_update(self._data, result)
            result = self
        return result
    
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
    
    def hide(self):
        '''Same as jQuery hide method.'''
        self._css.update({'display': 'none'})
        return self
    
    def show(self):
        '''Same as jQuery show method.'''
        self._css.pop('display', None)
        return self
    
    def do_stream(self, request):
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
    
    def _attrdata(self, cont, name, *val):
        if isinstance(name, Mapping):
            if val:
                raise TypeError('Cannot set a value to %s' % name)
            return name, True
        else:
            if val:
                if len(val) == 1:
                    return {name: val[0]}, True
                else:
                    raise TypeError('Too may arguments')
            else:
                return cont.get(name), False
            

class Media(AsyncString):
    
    def __init__(self, media_path):
        super(Media, self).__init__()
        self.media_path = media_path
        self._children = OrderedDict()
    
    def append(self, value):
        raise NotImplementedError
        
    def absolute_path(self, path):
        if path.startswith('http://') or path.startswith('https://')\
            or path.startswith('/'):
            return path
        return remove_double_slash('/%s/%s' % (self.media_path, path))


class Css(Media):
        
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

    def do_stream(self, request):
        for medium in sorted(self._children):
            paths = self._children[medium]
            medium = '' if medium == 'all' else " media='%s'" % medium
            for path in paths:
                link = "<link href='%s' type='text/css'%s rel='stylesheet'/>\n"\
                        % (self.absolute_path(path.link), medium)
                if path.condition:
                    link = '<!--[if %s]>%s<![endif]-->' % (path.condition, link)
                yield link
    
    
class Scripts(Media):
        
    def append(self, child):
        if child and is_string(child):
            path = self.absolute_path(child)
            script = Html('script', src=path, type='text/javascript')
            self._children[script] = script
            return script
    
    def do_stream(self, request):
        for child in self._children.values():
            for bit in child.stream(request):
                yield bit
            yield '\n'
        
        
class Head(Html):
    ''':class:`Html` head tag. It contains :class:`Html` handlers for the
various part of an HTML Head element.
    
.. attribute:: title

    Text in the title tag
    
.. attribute:: meta

    A container of :class:`Html` meta tags. To add new meta tags use the
    :meth:`add_meta` method rather than accessing the :attr:`meta`
    attribute directly.
    
.. attribute:: scripts

    A container of Javascript files to render at the end of the body tag.
    To add new javascript files simply use the append method on
    this attribute. You can add relative paths::
    
        html.head.scripts.append('/media/js/scripts.js')
    
    as well as absolute paths::
    
        html.head.scripts.append('https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js')

'''
    def __init__(self, media_path=None, title=None, meta=None, charset=None):
        super(Head, self).__init__('head')
        self.title = title
        self.append(Html(None, meta))
        self.append(Css(media_path))
        self.append(Scripts(media_path))
        self.add_meta(charset=charset or 'utf-8')
    
    @property
    def meta(self):
        return self._children[0]
    
    @property
    def links(self):
        return self._children[1]
    
    def __get_scripts(self):
        return self._children[2]
    def __set_scripts(self, scripts):
        self._children[2] = scripts
    scripts = property(__get_scripts, __set_scripts)
    
    def do_stream(self, request):
        if self.title:
            self._children.insert(0, '<title>%s</title>' % self.title)
        return super(Head, self).do_stream(request)
        
    def body(self, request):
        return self.js_body.content(request)
    
    def add_meta(self, **kwargs):
        '''Add a new :class:`Html` meta tag to the :attr:`meta` collection.'''
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
    ''':class:`Html` body tag.
    
.. attribute:: scripts

    A container of Javascript files to render at the end of the body tag.
    The usage is the same as :attr:`Head.scripts`.    
'''
    def __init__(self, media_path=None):
        super(Body, self).__init__('body')
        self.scripts = Scripts(media_path)
    
    def do_stream(self, request):
        '''Render the widget. It accept two optional parameters, a http
request object and a dictionary for rendering children with a key.

:parameter request: Optional request object.
'''
        self.append(self.scripts)
        return super(Body, self).do_stream(request)
                
                
class HtmlDocument(Html):
    '''HTML5 asynchronous document. An instance of this class can be obtained
via the :attr:`pulsar.apps.wsgi.wrappers.WsgiRequest.html_document` attribute.
    
.. attribute:: head

    The :class:`Head` part of this :class:`HtmlDocument`

.. attribute:: body

    The :class:`Body` part of this :class:`HtmlDocument`
    
'''
    def __init__(self, title=None, media_path='/media/', charset=None,
                 **params):
        super(HtmlDocument, self).__init__(None, **params)
        self.head = Head(title=title, media_path=media_path, charset=charset)
        self.body = Body(media_path=media_path)
    
    def __call__(self, title=None, body=None, media_path=None):
        if title:
            self.head.title = title
        if media_path:
            self.head.scripts.media_path = media_path
            self.head.links.media_path = media_path
            self.body.scripts.media_path = media_path
        self.body.append(body)
        return self
        
    @async()
    def do_stream(self, request):
        body = yield self.body.content(request)
        head = yield self.head.content(request)
        yield  ('<!DOCTYPE html>\n', '<html%s>\n' % self.flatatt(),
                head, '\n', body, '\n</html>')
