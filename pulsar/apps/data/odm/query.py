

class AbstractQuery(object):
    _meta = None

    def filter(self, **kwargs):
        raise NotImplementedError

    def exclude(self, **kwargs):
        raise NotImplementedError

    def union(self, *queries):
        raise NotImplementedError

    def intersect(self, *queries):
        raise NotImplementedError

    def where(self, *expressions):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def all(self):
        raise NotImplementedError


def query_op(f):
    '''Decorator for a :class:`Query` operation.
    '''
    name = f.__name__

    def _(self, *args, **kwargs):
        if self._store.has_query(name):
            q = self._clone()
            return f(q, *args, **kwargs)
        else:
            raise QueryError('Cannot use %s with %s' % name, self._store)

    _.__doc__ = f.__doc__
    _.__name__ = name
    return _


class Query(object):
    '''A query for data in a model store.

    A :class:`Query` is produced in terms of a given :class:`.Manager`,
    using the :meth:`~.Manager.query` method.
    '''
    _filters = None
    _joins = None
    _excludes = None
    _unions = None
    _intersections = None
    _where = None
    _execution = None

    def __init__(self, manager):
        self._manager = manager
        self._store = manager._read_store

    @property
    def _meta(self):
        return self._manager._meta

    @query_op
    def filter(self, **kwargs):
        '''Create a new :class:`Query` with additional clauses.

        The clauses corresponds to ``where`` or ``limit`` in a
        ``SQL SELECT`` statement.

        :params kwargs: dictionary of limiting clauses.

        For example::

            qs = manager.query().filter(group='planet')
        '''
        if kwargs:
            self._filters = update(self._filters, kwargs)
        return self

    @query_op
    def exclude(self, **kwargs):
        '''Create a new :class:`Query` with additional clauses.

        The clauses correspond to ``EXCEPT`` in a ``SQL SELECT`` statement.

        Using an equivalent example to the :meth:`filter` method::

            qs = manager.query()
            result1 = qs.exclude(group='planet')
            result2 = qs.exclude(group=('planet','stars'))

        '''
        if kwargs:
            self._excludes = update(self._excludes, kwargs)
        return self

    @query_op
    def union(self, *queries):
        '''Create a new :class:`Query` obtained form unions.

        :param queries: positional :class:`Query` parameters to create an
            union with.
        For example, lets say we want to have the union
        of two queries obtained from the :meth:`filter` method::

            query = mymanager.query()
            qs = query.filter(field1='bla').union(query.filter(field2='foo'))
        '''
        if queries:
            self._unions = update(self._unions, queries)
        return self

    @query_op
    def intersect(self, *queries):
        '''Create a new :class:`Query` obtained form intersection.

        :param queries: positional :class:`Query` parameters to create an
            intersection with.
        For example, lets say we want to have the intersection
        of two queries obtained from the :meth:`filter` method::

            query = mymanager.query()
            q1 = query.filter(field2='foo')
            qs = query.filter(field1='bla').intersect(q1)
        '''
        if queries:
            self._intersections = update(self._intersections, queries)
        return self

    @query_op
    def where(self, *expressions):
        if expressions:
            self._where = update(self._where, expressions)
        return self

    @query_op
    def join(self):
        raise NotImplementedError

    def compiled(self):
        if not self._execution:
            cm = self._manager.read_store.compiler()
            self._execution = cm.compile_query(self)
        return self._execution

    def count(self):
        '''Count the number of objects selected by this :class:`Query`.

        This method is efficient since the :class:`Query` does not
        receive any data from the server apart from the number of
        matched elements.'''
        return self.compiled().count()

    def _clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        d = q.__dict__
        for name, value in self.__dict__.items():
            if name not in ('_execution',):
                if isinstance(value, (list, dict)):
                    value = copy(value)
                d[name] = value
        return q
