'''
The :class:`.Template` defines a family of classes which can be used to
build HTML elements in a pythonic fashion. No template specific language is
required, instead a template for an html element is created by adding children
:class:`.Template` to a parent one.


Template
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Template
   :members:
   :member-order: bysource


Context
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Context
   :members:
   :member-order: bysource


Page Template
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PageTemplate
   :members:
   :member-order: bysource


Grid Template
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: GridTemplate
   :members:
   :member-order: bysource


Row Template
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: RowTemplate
   :members:
   :member-order: bysource

Column Template
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ColumnTemplate
   :members:
   :member-order: bysource
'''
from itertools import chain

from pulsar.utils.structures import AttributeDictionary

from .content import Html


__all__ = ['Template', 'Context', 'ColumnTemplate', 'RowTemplate',
           'GridTemplate', 'PageTemplate']


class Template(object):
    '''A factory of :class:`.Html` objects.

    ::

        >>> simple = Template(Context('foo', tag='span'), tag='div')
        >>> html = simple(cn='test', context={'foo': 'bla'})
        >>> html.render()
        <div class='test'><span data-context='foo'>bla</span></div>

    .. attribute:: tag

        An optional HTML tag_ for the outer element of this template.
        If not specified, this template is a container of other templates
        and no outer element is rendered.

    .. attribute:: key

        An optional string which identify this :class:`Template` within
        other templates. It is also used for extracting content from the
        ``context`` dictionary passed to the template callable method.

    .. attribute:: children

        List of :class:`Template` objects which are rendered as children
        of this :class:`Template`

    .. attribute:: parameters

        An attribute dictionary containing all key-valued parameters passed
        during initialisation. These parameters are used when building an
        :class:`.Html` element via the callable method.

        it is initialised by the :meth:`init_parameters` method at the end
        of initialisation.

    .. _tag: http://www.w3schools.com/tags/
    '''
    key = None
    tag = None
    classes = None

    def __init__(self, *children, **parameters):
        if 'key' in parameters:
            self.key = parameters.pop('key')
        if not children:
            children = [self.child_template()]
        new_children = []
        for child in children:
            child = self.child_template(child)
            if child:
                new_children.append(child)
        self.children = new_children
        self.init_parameters(**parameters)

    def __repr__(self):
        return '%s(%s)' % (self.key or self.__class__.__name__, self.tag or '')

    def __str__(self):
        return self.__repr__()

    def child_template(self, child=None):
        return child

    def init_parameters(self, tag=None, **parameters):
        '''Called at the and of initialisation.

        It fills the :attr:`parameters` attribute.
        It can be overwritten to customise behaviour.
        '''
        self.tag = tag or self.tag
        self.parameters = AttributeDictionary(parameters)

    def __call__(self, request=None, context=None, children=None, **kwargs):
        '''Create an Html element from this template.'''
        c = []
        if context is None:
            context = {}
        for child in self.children:
            child = child(request, context, **kwargs)
            c.append(self.post_process_child(child, **kwargs))
        if children:
            c.extend(children)
        return self.html(request, context, c, **kwargs)

    def html(self, request, context, children, **kwargs):
        '''Create the :class:`Html` instance.

        This method is invoked at the end of the ``__call__`` method with
        a list of ``children`` elements and a ``context`` dictionary.
        This method shouldn't be accessed directly.

        :param request: a client request, can be ``None``.
        :param context: a dictionary of :class:`Html` or strings to include.
        :param children: list of children elements.
        :param kwargs: additional parameters used when initialising the
            :attr:`Html` for this template.
        :return: an :class:`Html` object.
        '''
        params = self.parameters
        if kwargs:
            params = dict(params)
            params.update(kwargs)
        html = Html(self.tag, *children, **params)
        html.maker = self
        if context and self.key:
            html.append(context.get(self.key))
        classes = self.classes
        if hasattr(classes, '__call__'):
            classes = classes()
        return html.addClass(classes)

    def keys(self):
        '''Generator of keys in this :class:`.Template`
        '''
        for child in self.children:
            if child.key:
                yield child.key
            for key in child.keys():
                yield key

    def get(self, key):
        '''Retrieve a children :class:`Template` with :attr:`Template.key`
        equal to ``key``.

        The search is done recursively and the first match is
        returned. If not available return ``None``.
        '''
        for child in self.children:
            if child.key == key:
                return child
        for child in self.children:
            elem = child.get(key)
            if elem is not None:
                return elem

    def post_process_child(self, child, **parameters):
        return child


class Context(Template):
    '''A :class:`Template` which enforces the :attr:`~.Template.key`
    attribute.

    Fore example::

        >>> from lux import Context
        >>> template = Context('foo', tag='div')
        >>> template.key
        'foo'
        >>> html = template(context={'foo': 'pippo'})
        >>> html.render()
        <div>pippo</div>
    '''
    def __init__(self, key, *children, **params):
        params['key'] = key
        super(Context, self).__init__(*children, **params)


class ColumnTemplate(Template):
    '''A template to place inside a :class:`.RowTemplate`.

    :param span=1: fraction indicating how much this template extend across
        its row container. For example ``0.5`` is half span. Must be between
        0 and 1.

    :param device='md': The device which render the column with the correct
        ``span``. Valid options are

        * ``xs`` extra small (phones)
        * ``sm`` small (tablets)
        * ``md`` medium (desktops)
        * ``lg`` large (desktops)
    '''
    tag = 'div'

    def __init__(self, *children, **params):
        self.span = params.pop('span', 1)
        self.device = params.pop('device', 'md')
        super(ColumnTemplate, self).__init__(*children, **params)
        if self.span > 1:
            raise ValueError('Column span "%s" greater than one!' % self.span)


class RowTemplate(Template):
    '''A :class:`.RowTemplate` is a container of :class:`.ColumnTemplate`
    elements. It should be placed inside a :class:`.GridTemplate` for
    better rendering.

    :param column: Optional parameter which set the :attr:`column` attribute.

    .. attribute:: column

        It can be either 12 or 24 and it indicates the number of column
        spans available.

        Default: 12
    '''
    grid_child = True
    tag = 'div'
    classes = 'row'
    columns = 12

    def __init__(self, *children, **params):
        self.columns = params.pop('columns', self.columns)
        # self.classes = 'grid%s row' % self.columns
        super(RowTemplate, self).__init__(*children, **params)

    def child_template(self, child=None):
        if not isinstance(child, ColumnTemplate):
            child = ColumnTemplate(child)
        span = round(child.span * self.columns)
        child.classes = 'col-%s-%s' % (child.device, span)
        return child


class GridTemplate(Template):
    '''A container of :class:`.RowTemplate` or other templates.

    :parameter fixed: optional boolean flag to indicate if the grid is
        fixed (html class ``grid fixed``) or fluid (html class ``grid fluid``).
        If not specified the grid is considered fluid (it changes width when
        the browser window changes width).

    '''
    tag = 'div'

    def __init__(self, *children, **params):
        self.fixed = params.pop('fixed', True)
        super(GridTemplate, self).__init__(*children, **params)

    def html(self, request, context, children, **kwargs):
        html = super(GridTemplate, self).html(request, context, children,
                                              **kwargs)
        cn = 'container' if self.fixed else 'container-fluid'
        return html.addClass(cn)

    def child_template(self, child=None):
        if not getattr(child, 'grid_child', False):
            child = RowTemplate(child)
        return child


class PageTemplate(Template):
    '''A :class:`.Template` to render the inner part of the HTML ``body`` tag.

    A page template is created by including the page components during
    initialisation, for example::

        from lux.extensions.cms.grid import PageTemplate

        head_body_foot = PageTemplate(
            Template(...),
            GridTemplate(...),
            ...)
    '''
    tag = 'div'
    classes = 'cms-page'

    def __init__(self, *children, **params):
        params['role'] = 'page'
        super(PageTemplate, self).__init__(*children, **params)

    def html(self, request, context, children, **kwargs):
        html = super(PageTemplate, self).html(request, context, children,
                                              **kwargs)
        # if request:
        #     site_contents = []
        #     ids = context.get('content_ids')
        #     if ids:
        #         contents = yield from request.models.content.filter(
        #             id=ids).all()
        #         for content in contents:
        #             for elem in ids.get(content.id, ()):
        #                 self.apply_content(elem, content)
        # content = yield from html(request)
        # return content

    def apply_content(self, elem, content):
        elem.data({'id': content.id, 'content_type': content.content_type})
        for name, value in chain((('title', content.title),
                                  ('keywords', content.keywords)),
                                 content.data.items()):
            if isinstance(value, str):
                elem.append(Html('div', value, field=name))
            else:
                elem.data(name, value)
