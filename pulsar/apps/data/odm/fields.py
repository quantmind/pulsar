from copy import copy
from datetime import date, datetime
from base64 import b64encode

from pulsar.utils.html import UnicodeMixin
from pulsar.utils.pep import itervalues, pickle, to_string



class Field(UnicodeMixin):
    '''Base class of all :mod:`.odm` Fields.

    Each field is specified as a :class:`.Model` class attribute.

    .. attribute:: index

        Probably the most important field attribute, it establish if
        the field creates indexes for queries.
        If you don't need to query the field you should set this value to
        ``False``, it will save you memory.

        .. note:: if ``index`` is set to ``False`` executing queries
                  against the field will
                  throw a :class:`stdnet.QuerySetError` exception.
                  No database queries are allowed for non indexed fields
                  as a design decision (explicit better than implicit).

        Default ``True``.

    .. attribute:: unique

        If ``True``, the field must be unique throughout the model.
        In this case :attr:`Field.index` is also ``True``.
        Enforced at :class:`stdnet.BackendDataServer` level.

        Default ``False``.

    .. attribute:: primary_key

        If ``True``, this field is the primary key for the model.
        A primary key field has the following properties:

        * :attr:`Field.unique` is also ``True``.
        * There can be only one in a model.
        * It's attribute name in the model must be **id**.
        * If not specified a :class:`AutoIdField` will be added.

        Default ``False``.

    .. attribute:: required

        If ``False``, the field is allowed to be null.

        Default ``True``.

    .. attribute:: default

        Default value for this field. It can be a callable attribute with
        arity 0.

        Default ``None``.

    .. attribute:: name

        Field name, created by the ``odm`` at runtime.

    .. attribute:: attname

        The attribute name for the field, created by the :meth:`get_attname`
        method at runtime. For most field, its value is the same as the
        :attr:`name`. It is the field sorted in the backend database.

    .. attribute:: model

        The :class:`.Model` holding the field.
        Created by the ``odm`` at runtime.

    .. attribute:: charset

        The charset used for encoding decoding text.

    .. attribute:: hidden

        If ``True`` the field will be hidden from search algorithms.

        Default ``False``.

    .. attribute:: python_type

        The python ``type`` for the :class:`Field`.

    .. attribute:: as_cache

        If ``True`` the field contains data which is considered cache and
        therefore always reproducible. Field marked as cache, have
        :attr:`required` always ``False``.

        This attribute is used by the :class:`.Model.fieldvalue_pairs` method
        which returns a dictionary of field names and values.

        Default ``False``.
    '''
    to_python = None
    to_store = None
    index = False
    _default = None
    creation_counter = 0

    def __init__(self, unique=False, primary_key=False, required=True,
                 index=None, hidden=None, as_cache=False, **extras):
        self.primary_key = primary_key
        index = index if index is not None else self.index
        if primary_key:
            self.unique = True
            self.required = True
            self.index = True
            self.as_cache = False
            extras['default'] = None
        else:
            self.unique = unique
            self.required = required
            self.as_cache = as_cache
            self.index = True if unique else index
        if self.as_cache:
            self.required = False
            self.unique = False
            self.index = False
        self.meta = None
        self.name = None
        self.model = None
        self._default = extras.pop('default', self._default)
        self._handle_extras(**extras)
        self.creation_counter = Field.creation_counter
        Field.creation_counter += 1

    def register_with_model(self, name, model):
        '''Called during the creation of a the :class:`StdModel`
        class when :class:`Metaclass` is initialised. It fills
        :attr:`Field.name` and :attr:`Field.model`. This is an internal
        function users should never call.'''
        if self.name:
            raise FieldError('Field %s is already registered\
 with a model' % self)
        self.name = name
        self.attname = self.get_attname()
        self.model = model
        meta = model._meta
        self.meta = meta
        meta.dfields[name] = self
        if self.primary_key:
            model._meta.pk = self

    def get_attname(self):
        '''Generate the :attr:`attname` at runtime'''
        return self.name

    def _handle_extras(self, **extras):
        '''Callback to hadle extra arguments during initialization.'''
        self.error_extras(extras)

    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError(("__init__() got an unexepcted keyword argument "
                             "'{0}'".format(keys[0])))


class CharField(Field):

    def to_python(self, value):
         return value.decode('utf-8', errors='ignore')


class AutoIdField(Field):
    pass


class IntegerField(Field):

    def to_python(self, value):
        try:
            return int(value)
        except Exception:
            return None


class FloatField(Field):

    def to_python(self, value):
        try:
            return float(value)
        except Exception:
            return None


class PickleField(Field):

    def to_python(self, value):
        try:
            return pickle.loads(value)
        except exception:
            return None

    def to_store(self, value):
        try:
            return pickle.dumps(value, protocol=2)
        except exception:
            return None
