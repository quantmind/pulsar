import pickle
from functools import partial
from base64 import b64encode
from datetime import date, datetime

from pulsar.utils.html import UnicodeMixin
from pulsar.utils.numbers import date2timestamp

from .manager import (OneToManyRelatedManager, load_relmodel, LazyForeignKey,
                      OdmError)


NONE_EMPTY = (None, '', b'')


class FieldError(OdmError):
    pass


class FieldValueError(ValueError):
    pass


def sql():
    import sqlalchemy
    return sqlalchemy


class Field(UnicodeMixin):
    '''Base class of all :mod:`.odm` Fields.

    Each field is specified as a :class:`.Model` class attribute.

    .. attribute:: index

        Some data stores requires to create indexes when performing specific
        queries.

        Default ``False``.

    .. attribute:: unique

        If ``True``, the field must be unique throughout the model.
        In this case :attr:`~Field.index` is also ``True``.

        Default ``False``.

    .. attribute:: primary_key

        If ``True``, this field is the primary key for the model.

        A primary key field has the following properties:

        * :attr:`~Field.unique` is also ``True``
        * There can be only one in a model
        * If not specified a :class:`.AutoIdField` will be added

        Default ``False``.

    .. attribute:: required

        If ``False``, the field is allowed to be null.

        Default ``True``.

    .. attribute:: default

        Default value for this field. It can be a callable attribute.

        Default ``None``.

    .. attribute:: name

        Field name, created by the ``odm`` at runtime.

    .. attribute:: store_name

        The name for the field, created by the :meth:`get_store_name`
        method at runtime. For most field, its value is the same as the
        :attr:`name`. It is the field stored in the backend database.

    .. attribute:: _meta

        The :class:`.ModelMeta` holding the field.
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
    repr_type = 'unknown'
    primary_key = False
    autoincrement = False
    required = True
    to_python = None
    index = False
    _default = None
    creation_counter = 0

    def __init__(self, unique=False, primary_key=None, required=None,
                 index=None, hidden=False, as_cache=False, **extras):
        self.foreign_keys = ()
        self.primary_key = (self.primary_key if primary_key is None else
                            primary_key)
        if required is None:
            required = self.required
        index = index if index is not None else self.index
        if self.primary_key:
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
        self.hidden = hidden
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
        self.store_name = self.get_store_name()
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
            self._meta.indexes.append(self)

    def get_store_name(self):
        '''Generate the :attr:`store_name` at runtime'''
        return self.name

    def get_value(self, instance, value):
        return value

    def to_store(self, value, store=None):
        if value in NONE_EMPTY:
            return self.get_default()
        else:
            return value

    def to_json(self, value, store=None):
        return value

    def get_default(self):
        '''Default value for this :class:`.Field`
        '''
        if hasattr(self._default, '__call__'):
            return self._default()
        else:
            return self._default

    def _handle_extras(self, **extras):
        '''Callback to hadle extra arguments during initialization.'''
        self.error_extras(extras)

    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError(("__init__() got an unexepcted keyword argument "
                             "'{0}'".format(keys[0])))

    def sql_alchemy_column(self):
        '''Return a valid Column for :sqlalchemy:`SqlAlchemy model <>`.
        '''
        raise NotImplementedError


class CharField(Field):
    repr_type = 'text'

    def to_python(self, value, store=None):
        if isinstance(value, bytes):
            return value.decode('utf-8', 'ignore')
        elif value is not None:
            return str(value)
    to_store = to_python
    to_json = to_python

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.String, primary_key=self.primary_key,
                        default=self._default, nullable=not self.required)


class AutoIdField(Field):
    primary_key = True

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Integer, primary_key=self.primary_key,
                        default=self._default, nullable=not self.required)


class IntegerField(Field):
    index = True
    repr_type = 'numeric'

    def to_python(self, value, store=None):
        try:
            return int(value)
        except Exception:
            return None

    def to_store(self, value, store=None):
        if value not in NONE_EMPTY:
            return int(value)
        else:
            return self.get_default()

    to_json = to_python

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Integer, primary_key=self.primary_key,
                        autoincrement=self.autoincrement,
                        default=self._default, nullable=not self.required)


class BooleanField(Field):
    index = True
    repr_type = 'bool'

    def to_python(self, value, store=None):
        try:
            return bool(int(value))
        except Exception:
            return None
    to_json = to_python

    def to_store(self, value, store):
        if value in NONE_EMPTY:
            value = self.get_default()
        return store.encode_bool(value)

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Boolean, primary_key=self.primary_key,
                        default=self._default,
                        nullable=not self.required)


class FloatField(Field):
    index = True
    repr_type = 'numeric'

    def to_python(self, value, store=None):
        try:
            return float(value)
        except Exception:
            return None
    to_json = to_python

    def to_store(self, value, store=None):
        if value not in NONE_EMPTY:
            return float(value)
        else:
            return self.get_default()

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Float, default=self._default,
                        nullable=not self.required)


class DateField(Field):
    repr_type = 'numeric'

    def to_python(self, value, backend=None):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                if isinstance(value, datetime):
                    value = value.date()
            else:
                value = datetime.fromtimestamp(float(value)).date()
            return value
        else:
            return self.get_default()

    def to_store(self, value, backend=None):
        if value in NONE_EMPTY:
            value = self.get_default()
        if isinstance(value, date):
            return date2timestamp(value)
        elif value not in NONE_EMPTY:
            raise FieldValueError('%s not a valid date' % value)
    to_json = to_store

    def _handle_extras(self, auto_now=False, **extras):
        self.auto_now = auto_now
        super(DateField, self)._handle_extras(**extras)

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Date, default=self._default,
                        nullable=not self.required)


class DateTimeField(DateField):

    def to_python(self, value, backend=None):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                if not isinstance(value, datetime):
                    value = datetime(value.year, value.month, value.day)
            else:
                value = datetime.fromtimestamp(float(value))
            return value
        else:
            return self.get_default()

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.DateTime, default=self._default,
                        nullable=not self.required)


class PickleField(Field):
    repr_type = 'bytes'
    required = False
    index = False

    def to_python(self, value, store=None):
        if value is not None:
            if store:
                value = store.decode_bytes(value)
            if isinstance(value, bytes):
                return pickle.loads(value)
            else:
                return value

    def to_store(self, value, store=None):
        if value is not None:
            try:
                value = pickle.dumps(value, protocol=2)
                if store:
                    value = store.encode_bytes(value)
                return value
            except Exception:
                return None

    def to_json(self, value, store=None):
        if isinstance(value, (int, float, str, tuple, list, dict)):
            return value
        else:
            value = self.to_store(value)
            if value is not None:
                return b64encode(value).decode('utf-8')

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.PickleType, default=self._default,
                        nullable=not self.required)


class JSONField(CharField):
    '''A JSON field which implements automatic conversion to
    and from an object and a JSON string.

    It is the responsability of the
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
    required = False
    index = False

    def to_store(self, value, store):
        if value is NONE_EMPTY:
            value = self.get_default()
        return store.encode_json(value)

    def to_python(self, value, store=None):
        if value is None:
            return self.get_default()
        if isinstance(value, str):
            try:
                return json.loads(value)
            except TypeError:
                return None
        else:
            return value

    def serialise(self, value, lookup=None):
        if lookup:
            value = range_lookups[lookup](value)
        return self.encoder.dumps(value)

    def value_from_data(self, instance, data):
        if self.as_string:
            return data.pop(self.store_name, None)
        else:
            return flat_to_nested(data, instance=instance,
                                  store_name=self.store_name,
                                  loads=self.encoder.loads)

    def get_sorting(self, name, errorClass):
        pass

    def get_lookup(self, name, errorClass):
        if self.as_string:
            return super(JSONField, self).get_lookup(name, errorClass)
        else:
            if name:
                name = JSPLITTER.join((self.store_name, name))
            return (name, None)

    def sql_alchemy_column(self):
        s = sql()
        return s.Column(s.Text, default=self._default,
                        nullable=not self.required)


class ForeignKey(Field):
    '''A :class:`.Field` defining a :ref:`one-to-many <one-to-many>`
    objects relationship.

    It requires a positional argument representing the :class:`.Model`
    to which the model containing this field is related. For example::

        class Folder(odm.Model):
            name = odm.CharField()

        class File(odm.Model):
            folder = odm.ForeignKey(Folder, related_name='files')

    To create a recursive relationship, an object that has a many-to-one
    relationship with itself use::

        odm.ForeignKey('self')

    Behind the scenes, the :ref:`odm <odm>` appends ``_id`` to the field
    name to create
    its field name in the back-end data-server. In the above example,
    the database field for the ``File`` model will have a ``folder_id`` field.

    .. attribute:: related_name

        Optional name to use for the relation from the related object
        back to ``self``.
    '''
    index = True
    proxy_class = LazyForeignKey
    related_manager_class = OneToManyRelatedManager

    def __init__(self, model, related_name=None, related_manager_class=None,
                 **kwargs):
        if related_manager_class:
            self.related_manager_class = related_manager_class
        if not model:
            raise FieldError('Model not specified')
        super(ForeignKey, self).__init__(**kwargs)
        self.relmodel = model
        self.related_name = related_name

    def register_with_related_model(self):
        # add the RelatedManager proxy to the model holding the field
        setattr(self._meta.model, self.name, self.proxy_class(self))
        # self._meta.related[self.name] =
        load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel, **kw):
        self._relmeta = meta = relmodel._meta
        if not self.related_name:
            self.related_name = '%s_%s_set' % (self._meta.name, self.name)
        if (self.related_name not in meta.related and
                self.related_name not in meta.dfields):
            self._relmeta.related[self.related_name] = self
        else:
            raise FieldError('Duplicated related name "{0} in model "{1}" '
                             'and field {2}'.format(self.related_name,
                                                    meta, self))

    def get_store_name(self):
        return '%s_id' % self.name

    def get_value(self, instance, value):
        if isinstance(value, self.relmodel):
            id = value.id
            if id:
                instance['_%s' % self.name] = value
            return id
        else:
            return value

    def to_store(self, value, store):
        if isinstance(value, self.relmodel):
            return value.id
        else:
            return value

    def register_with_model(self, name, model):
        super(ForeignKey, self).register_with_model(name, model)
        if not model._meta.abstract:
            self.register_with_related_model()


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
    primary_key = True

    def __init__(self, *fields, **kwargs):
        super(CompositeIdField, self).__init__(**kwargs)
        self.fields = fields
        if len(self.fields) < 2:
            raise FieldError('At least two fields are required by composite '
                             'CompositeIdField')

    def register_with_model(self, name, model):
        fields = []
        for field in self.fields:
            if field not in model._meta.dfields:
                raise FieldError(
                    'Composite id field "%s" not in in "%s" model.' %
                    (field, model._meta))
            field = model._meta.dfields[field]
            fields.append(field)
        self.fields = tuple(fields)
        return super(CompositeIdField, self).register_with_model(name, model)


class ManyToManyField(Field):
    '''A :ref:`many-to-many <many-to-many>` relationship.
Like :class:`ForeignKey`, it requires a positional argument, the class
to which the model is related and it accepts **related_name** as extra
argument.

.. attribute:: related_name

    Optional name to use for the relation from the related object
    back to ``self``. For example::

        class Group(odm.StdModel):
            name = odm.SymbolField(unique=True)

        class User(odm.StdModel):
            name = odm.SymbolField(unique=True)
            groups = odm.ManyToManyField(Group, related_name='users')

    To use it::

        >>> g = Group(name='developers').save()
        >>> g.users.add(User(name='john').save())
        >>> u.users.add(User(name='mark').save())

    and to remove::

        >>> u.following.remove(User.objects.get(name='john'))

.. attribute:: through

    An optional :class:`StdModel` to use for creating the many-to-many
    relationship can be passed to the constructor, via the **through** keyword.
    If such a model is not passed, under the hood, a :class:`ManyToManyField`
    creates a new *model* with name constructed from the field name
    and the model holding the field. In the example above it would be
    *group_user*.
    This model contains two :class:`ForeignKeys`, one to model holding the
    :class:`ManyToManyField` and the other to the *related_model*.

'''
    def __init__(self, model, through=None, related_name=None, **kwargs):
        self.through = through
        self.relmodel = model
        self.related_name = related_name
        super(ManyToManyField, self).__init__(model, **kwargs)

    def register_with_model(self, name, model):
        super(ManyToManyField, self).register_with_model(name, model)
        if not model._meta.abstract:
            load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel):
        relmodel._manytomany_through_model(self)

    def get_store_name(self):
        return None

    def todelete(self):
        return False

    def add_to_fields(self):
        # A many to many field is a dummy field. All it does it provides a
        # proxy for the through model. Remove it from the fields dictionary
        # and addit to the list of many_to_many
        self._meta.dfields.pop(self.name)
        self._meta.manytomany.append(self.name)
