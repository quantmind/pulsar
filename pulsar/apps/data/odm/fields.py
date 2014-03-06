import pickle
from base64 import b64encode

from pulsar.utils.html import UnicodeMixin


class FieldError(RuntimeError):
    pass


def get_field_type(field):
    return getattr(field, 'repr_type', 'text')


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
        self.foreign_keys = ()
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
        self._meta = None
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
        assert not self.name, 'Field %s is already registered' % self
        self.name = name
        self.attname = self.get_attname()
        self.model = model
        self._meta = meta = model._meta
        meta.dfields[name] = self
        if self.to_python:
            meta.converters[name] = self.to_python
        if self.primary_key:
            meta.pk = self
        self.add_to_fields()

    def add_to_fields(self):
        '''Add this :class:`Field` to the fields of :attr:`model`.
        '''
        self._meta.scalarfields.append(self)
        if self.index:
            self._meta.indices.append(self)

    def get_attname(self):
        '''Generate the :attr:`attname` at runtime'''
        return self.name

    def to_json(self, value, store=None):
        return value

    def _handle_extras(self, **extras):
        '''Callback to hadle extra arguments during initialization.'''
        self.error_extras(extras)

    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError(("__init__() got an unexepcted keyword argument "
                             "'{0}'".format(keys[0])))


class CharField(Field):

    def to_python(self, value, store=None):
        if isinstance(value, bytes):
            return value.decode('utf-8', 'ignore')
        elif value is not None:
            return str(value)
    to_store = to_python
    to_json = to_python


class AutoIdField(Field):
    pass


class IntegerField(Field):
    repr_type = 'numeric'

    def to_python(self, value, store=None):
        try:
            return int(value)
        except Exception:
            return None
    to_store = to_python
    to_json = to_python


class BooleanField(Field):
    repr_type = 'bool'

    def to_python(self, value, store=None):
        try:
            return bool(int(value))
        except Exception:
            return None
    to_json = to_python

    def to_store(self, value, store=None):
        try:
            return 1 if value else 0
        except Exception:
            return None


class FloatField(Field):
    repr_type = 'numeric'

    def to_python(self, value, store=None):
        try:
            return float(value)
        except Exception:
            return None
    to_store = to_python
    to_json = to_python


class PickleField(Field):

    def to_python(self, value, store=None):
        if value is not None:
            try:
                return pickle.loads(value)
            except Exception:
                return None

    def to_store(self, value, store=None):
        if value is not None:
            try:
                return pickle.dumps(value, protocol=2)
            except Exception:
                return None

    def to_json(self, value, store=None):
        if isinstance(value, (int, float, str, tuple, list, dict)):
            return value
        else:
            value = self.to_store(value)
            if value is not None:
                return b64encode(value).decode('utf-8')


class JSONField(CharField):
    '''A JSON field which implements automatic conversion to
and from an object and a JSON string. It is the responsability of the
user making sure the object is JSON serializable.

There are few extra parameters which can be used to customize the
behaviour and how the field is stored in the back-end server.

:parameter encoder_class: The JSON class used for encoding.

    Default: :class:`stdnet.utils.jsontools.JSONDateDecimalEncoder`.

:parameter decoder_hook: A JSON decoder function.

    Default: :class:`stdnet.utils.jsontools.date_decimal_hook`.

:parameter as_string: Set the :attr:`as_string` attribute.

    Default ``True``.

.. attribute:: as_string

    A boolean indicating if data should be serialized
    into a single JSON string or it should be used to create several
    fields prefixed with the field name and the double underscore ``__``.

    Default ``True``.

    Effectively, a :class:`JSONField` with ``as_string`` attribute set to
    ``False`` is a multifield, in the sense that it generates several
    field-value pairs. For example, lets consider the following::

        class MyModel(odm.StdModel):
            name = odm.SymbolField()
            data = odm.JSONField(as_string=False)

    And::

        >>> m = MyModel(name='bla',
        ...             data={'pv': {'': 0.5, 'mean': 1, 'std': 3.5}})
        >>> m.cleaned_data
        {'name': 'bla', 'data__pv': 0.5, 'data__pv__mean': '1',
         'data__pv__std': '3.5', 'data': '""'}
        >>>

    The reason for setting ``as_string`` to ``False`` is to allow
    the :class:`JSONField` to define several fields at runtime,
    without introducing new :class:`Field` in your model class.
    These fields behave exactly like standard fields and therefore you
    can, for example, sort queries with respect to them::

        >>> MyModel.objects.query().sort_by('data__pv__std')
        >>> MyModel.objects.query().sort_by('-data__pv')

    which can be rather useful feature.
'''
    _default = {}

    def to_python(self, value, backend=None):
        if value is None:
            return self.get_default()
        try:
            return self.encoder.loads(value)
        except TypeError:
            return value

    def serialise(self, value, lookup=None):
        if lookup:
            value = range_lookups[lookup](value)
        return self.encoder.dumps(value)

    def value_from_data(self, instance, data):
        if self.as_string:
            return data.pop(self.attname, None)
        else:
            return flat_to_nested(data, instance=instance,
                                  attname=self.attname,
                                  loads=self.encoder.loads)

    def get_sorting(self, name, errorClass):
        pass

    def get_lookup(self, name, errorClass):
        if self.as_string:
            return super(JSONField, self).get_lookup(name, errorClass)
        else:
            if name:
                name = JSPLITTER.join((self.attname, name))
            return (name, None)


class CompositeIdField(Field):
    '''This field can be used when an instance of a model is uniquely
    identified by a combination of two or more :class:`Field` in the model
    itself. It requires a number of positional arguments greater or equal 2.
    These arguments must be fields names in the model where the
    :class:`CompositeIdField` is defined.

    .. attribute:: fields

        list of :class:`Field` names which are used to uniquely identify a
        model instance

    Check the :ref:`composite id tutorial <tutorial-compositeid>` for more
    information and tips on how to use it.
    '''
    type = 'composite'

    def __init__(self, *fields, **kwargs):
        super(CompositeIdField, self).__init__(**kwargs)
        self.fields = fields
        if len(self.fields) < 2:
            raise FieldError('At least tow fields are required by composite '
                             'CompositeIdField')

    def get_value(self, instance, *bits):
        if bits:
            raise AttributeError
        values = tuple((getattr(instance, f.attname) for f in self.fields))
        return hash(values)

    def register_with_model(self, name, model):
        fields = []
        for field in self.fields:
            if field not in model._meta.dfields:
                raise FieldError('Composite id field "%s" in in "%s" model.' %
                                 (field, model._meta))
            field = model._meta.dfields[field]
            fields.append(field)
        self.fields = tuple(fields)
        return super(CompositeIdField, self).register_with_model(name, model)
