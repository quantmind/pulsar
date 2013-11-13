from copy import copy
from datetime import date, datetime
from base64 import b64encode

from pulsar.utils.html import UnicodeMixin
from pulsar.utils.pep import string_type, to_string

from .exceptions import FieldException
from . import related


NONE_EMPTY = (None, '')


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
    _default = None
    type = None
    python_type = None
    index = True
    charset = None
    hidden = False
    internal_type = None
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
        self.charset = extras.pop('charset', self.charset)
        self.hidden = hidden if hidden is not None else self.hidden
        self.meta = None
        self.name = None
        self.model = None
        self._default = extras.pop('default', self._default)
        self._handle_extras(**extras)
        self.creation_counter = Field.creation_counter
        Field.creation_counter += 1

    @property
    def default(self):
        return self._default

    @property
    def class_field(self):
        return False

    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError('%s.__init__() got an unexepcted keyword argument '
                            '"%s"' % (self.__class__.__name__, keys[0]))

    def __unicode__(self):
        return to_string('%s.%s' % (self.meta, self.name))

    def value_from_data(self, instance, data):
        return data.pop(self.attname, None)

    def register_with_model(self, name, model):
        '''Called during the creation of a the :class:`.Model`
class when :class:`Metaclass` is initialised. It fills
:attr:`Field.name` and :attr:`Field.model`. This is an internal
function users should never call.'''
        if self.name:
            raise FieldException('Field %s is already registered with a '
                                 'model' % self)
        self.name = name
        self.attname = self.get_attname()
        self.model = model
        meta = model._meta
        self.meta = meta
        meta.dfields[name] = self
        meta.fields.append(self)
        if not self.primary_key:
            self.add_to_fields()
        else:
            model._meta.pk = self

    def add_to_fields(self):
        '''Add this :class:`Field` to the fields of :attr:`model`.'''
        meta = self.model._meta
        meta.scalarfields.append(self)
        if self.index:
            meta.indices.append(self)

    def get_attname(self):
        '''Generate the :attr:`attname` at runtime'''
        return self.name

    def get_cache_name(self):
        '''name for the private attribute which contains a cached value
for this field. Used only by realted fields.'''
        return '_%s_cache' % self.name

    def id(self, obj):
        '''Field id for object *obj*, if applicable. Default is ``None``.'''
        return None

    def get_default(self):
        "Returns the default value for this field."
        default = self._default
        if hasattr(default, '__call__'):
            return default()
        else:
            return default

    def __deepcopy__(self, memodict):
        '''Nothing to deepcopy here'''
        return copy(self)

    def __copy__(self):
        cls = self.__class__
        field = cls.__new__(cls)
        field.__dict__ = self.__dict__.copy()
        field.name = None
        field.model = None
        field.meta = None
        return field

    def filter(self, session, name, value):
        pass

    def get_sorting(self, name, errorClass):
        raise errorClass('Cannot use nested sorting on field {0}'.format(self))

    def get_lookup(self, remaining, errorClass=ValueError):
        '''called by the :class:`Query` method when it needs to build
lookup on fields with additional nested fields. This is the case of
:class:`ForeignKey` and :class:`JSONField`.

:param remaining: the :ref:`double underscored` fields if this :class:`Field`
:param errorClass: Optional exception class to use if the *remaining* field
    is not valid.'''
        if remaining:
            raise errorClass('Cannot use nested lookup on field %s' % self)
        return (self.attname, None)

    def todelete(self):
        return False

    ########################################################################
    ##    FIELD VALUES
    ########################################################################
    def get_value(self, instance, *bits):
        '''Retrieve the value :class:`Field` from a :class:`.Model`
``instance``.

:param instance: The :class:`.Model` ``instance`` invoking this function.
:param bits: Additional information for nested fields which derives from
    the :ref:`double underscore <tutorial-underscore>` notation.
:return: the value of this :class:`Field` in the ``instance``. can raise
    :class:`AttributeError`.

This method is used by the :meth:`.Model.get_attr_value` method when
retrieving values form a :class:`.Model` instance.
'''
        if bits:
            raise AttributeError
        else:
            return getattr(instance, self.attname)

    def set_value(self, instance, value):
        '''Set the ``value`` for this :class:`Field` in a ``instance``
of a :class:`.Model`.'''
        setattr(instance, self.attname, self.to_python(value))

    def set_get_value(self, instance, value):
        '''Set the ``value`` for this :class:`Field` in a ``instance``
of a :class:`.Model` and return the database representation. This method
is invoked by the validation lagorithm before saving instances.'''
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        return value

    def to_python(self, value, backend=None):
        """Converts the input value into the expected Python
data type, raising :class:`stdnet.FieldValueError` if the data
can't be converted.
Returns the converted value. Subclasses should override this."""
        return value

    def serialise(self, value, lookup=None):
        '''Convert ``value`` to a valid database representation for this field.

This method is invoked by the Query algorithm.'''
        return self.to_python(value)

    def json_serialise(self, value):
        '''Return a representation of this field which is compatible with
 JSON.'''
        return None

    def scorefun(self, value):
        '''Function which evaluate a score from the field value. Used by
the ordering alorithm'''
        return self.to_python(value)

    ########################################################################
    ##    TOOLS
    ########################################################################

    def _handle_extras(self, **extras):
        '''Callback to hadle extra arguments during initialization.'''
        self.error_extras(extras)


class AtomField(Field):
    '''The base class for fields containing ``atoms``.
An atom is an irreducible
value with a specific data type. it can be of four different types:

* boolean
* integer
* date
* datetime
* floating point
* symbol
'''
    def to_python(self, value, backend=None):
        if hasattr(value, '_meta'):
            return value.pkvalue()
        else:
            return value
    json_serialise = to_python


class AutoIdField(AtomField):
    '''An :class:`AtomField` for primary keys which are automatically
generated by the backend server.
You usually won't need to use this directly; a ``primary_key`` field
of this type, named ``id``, will automatically be added to your model
if you don't specify otherwise.
Check the :ref:`primary key tutorial <tutorial-primary-unique>` for
further information on primary keys.'''
    type = 'auto'

    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(AutoIdField, self).__init__(*args, **kwargs)

    def to_python(self, value, backend=None):
        if hasattr(value, '_meta'):
            return value.pkvalue()
        elif backend:
            return backend.auto_id_to_python(value)
        else:
            return value


class BooleanField(AtomField):
    '''A boolean :class:`AtomField`'''
    type = 'bool'
    internal_type = 'numeric'
    python_type = bool
    _default = False

    def __init__(self, required=False, **kwargs):
        super(BooleanField, self).__init__(required=required, **kwargs)

    def to_python(self, value, backend=None):
        if value in NONE_EMPTY:
            return self.get_default()
        else:
            return self.python_type(int(value))

    def set_get_value(self, instance, value):
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        return 1 if value else 0

    def serialise(self, value, lookup=None):
        return 1 if value else 0
    scorefun = serialise


class IntegerField(AtomField):
    '''An integer :class:`AtomField`.'''
    type = 'integer'
    internal_type = 'numeric'
    python_type = int

    def to_python(self, value, backend=None):
        if value in NONE_EMPTY:
            return self.get_default()
        else:
            return self.python_type(value)


class FloatField(IntegerField):
    '''An floating point :class:`AtomField`. By default
its :attr:`Field.index` is set to ``False``.
    '''
    type = 'float'
    internal_type = 'numeric'
    index = False
    python_type = float


class DateField(AtomField):
    '''An :class:`AtomField` represented in Python by
a :class:`datetime.date` instance.'''
    type = 'date'
    internal_type = 'numeric'
    python_type = date
    _default = None

    def set_get_value(self, instance, value):
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        return self.serialise(value)

    def to_python(self, value, backend=None):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                if isinstance(value, datetime):
                    value = value.date()
            else:
                value = timestamp2date(float(value)).date()
            return value
        else:
            return self.get_default()

    def serialise(self, value, lookup=None):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                value = date2timestamp(value)
            else:
                raise FieldValueError('Field %s is not a valid date' % self)
        return value
    scorefun = serialise
    json_serialise = serialise


class DateTimeField(DateField):
    '''A date :class:`AtomField` represented in Python by
a :class:`datetime.datetime` instance.'''
    type = 'datetime'
    python_type = datetime
    index = False

    def to_python(self, value, backend=None):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                if not isinstance(value, datetime):
                    value = datetime(value.year, value.month, value.day)
            else:
                value = timestamp2date(float(value))
            return value
        else:
            return self.get_default()


class SymbolField(AtomField):
    '''An :class:`AtomField` which contains a ``symbol``.
A symbol holds a unicode string as a single unit.
A symbol is irreducible, and are often used to hold names, codes
or other entities. They are indexes by default.'''
    type = 'text'
    python_type = string_type
    internal_type = 'text'
    _default = ''

    def to_python(self, value, backend=None):
        if value is not None:
            return self.encoder.loads(value)
        else:
            return self.get_default()

    def scorefun(self, value):
        raise FieldValueError('Could not obtain score')


class CharField(SymbolField):
    '''A text :class:`SymbolField` which is never an index.
It contains unicode and by default and :attr:`Field.required`
is set to ``False``.'''
    def __init__(self, *args, **kwargs):
        kwargs['index'] = False
        kwargs['unique'] = False
        kwargs['primary_key'] = False
        self.max_length = kwargs.pop('max_length', None)  # not used for now
        required = kwargs.get('required', None)
        if required is None:
            kwargs['required'] = False
        super(CharField, self).__init__(*args, **kwargs)


class BytesField(CharField):
    '''A :class:`Field` which contains binary data.'''
    type = 'bytes'
    internal_type = 'bytes'
    python_type = bytes
    _default = b''

    def json_serialise(self, value):
        if value is not None:
            return b64encode(self.to_python(value)).decode(self.charset)


class PickleObjectField(BytesField):
    '''A field which implements automatic conversion to and form a picklable
python object.
This field is python specific and therefore not of much use
if accessed from external programs. Consider the :class:`ForeignKey`
or :class:`JSONField` fields as more general alternatives.

.. note:: The best way to use this field is when its :class:`Field.as_cache`
          attribute is ``True``.
'''
    type = 'object'
    _default = None

    def set_get_value(self, instance, value):
        # Optimisation, avoid to call serialise since it is the same
        # as to_python
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        return self.serialise(value)

    def serialise(self, value, lookup=None):
        if value is not None:
            return self.encoder.dumps(value)

    def get_encoder(self, params):
        return encoders.PythonPickle(protocol=2)


class ForeignKey(Field):
    '''A field defining a :ref:`one-to-many <one-to-many>` relationship.

    Requires a positional argument: the class to which the model is related.
    For example::

        class Folder(odm.Model):
            name = odm.SymobolField()

        class File(odm.Model):
            folder = odm.ForeignKey(Folder, related_name = 'files')

    To create a recursive relationship, an object that has a many-to-one
    relationship with itself use::

        odm.ForeignKey('self')

    Behind the scenes, stdnet appends "_id" to the field name to create
    its field name in the back-end data-server. In the above example,
    the database field for the ``File`` model will have a ``folder_id`` field.

    .. attribute:: related_name

        Optional name to use for the relation from the related object
        back to ``self``.
    '''
    type = 'related object'
    internal_type = 'numeric'
    python_type = int
    proxy_class = related.LazyForeignKey
    related_manager_class = related.One2ManyRelatedManager

    def __init__(self, model, related_name=None, related_manager_class=None,
                 **kwargs):
        if related_manager_class:
            self.related_manager_class = related_manager_class
        super(ForeignKey, self).__init__(**kwargs)
        if not model:
            raise FieldException('Model not specified')
        self.relmodel = model
        self.related_name = related_name

    def register_with_related_model(self):
        # add the RelatedManager proxy to the model holding the field
        setattr(self.model, self.name, self.proxy_class(self))
        related.load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        meta = self.relmodel._meta
        if not self.related_name:
            self.related_name = '%s_%s_set' % (self.model._meta.name,
                                               self.name)
        if (self.related_name not in meta.related and
                self.related_name not in meta.dfields):
            self._register_with_related_model()
        else:
            raise FieldException('Duplicated related name "{0} in model "{1}" '
                                 'and field {2}'.format(self.related_name,
                                                        meta, self))

    def _register_with_related_model(self):
        manager = self.related_manager_class(self)
        setattr(self.relmodel, self.related_name, manager)
        self.relmodel._meta.related[self.related_name] = manager
        self.relmodel_manager = manager

    def get_attname(self):
        return '%s_id' % self.name

    def get_value(self, instance, *bits):
        related = getattr(instance, self.name)
        return related.get_attr_value(JSPLITTER.join(bits)
                                      ) if bits else related

    def set_value(self, instance, value):
        if isinstance(value, self.relmodel):
            setattr(instance, self.name, value)
            return value
        else:
            return super(ForeignKey, self).set_value(instance, value)

    def register_with_model(self, name, model):
        super(ForeignKey, self).register_with_model(name, model)
        if not model._meta.abstract:
            self.register_with_related_model()

    def scorefun(self, value):
        if isinstance(value, self.relmodel):
            return value.scorefun()
        else:
            raise FieldValueError('cannot evaluate score of {0}'.format(value))

    def to_python(self, value, backend=None):
        if isinstance(value, self.relmodel):
            return value.pkvalue()
        elif value:
            return self.relmodel._meta.pk.to_python(value, backend)
        else:
            return value
    json_serialise = to_python

    def filter(self, session, name, value):
        fname = name.split(JSPLITTER)[0]
        if fname in self.relmodel._meta.dfields:
            return session.query(self.relmodel, fargs={name: value})

    def get_sorting(self, name, errorClass):
        return self.relmodel._meta.get_sorting(name, errorClass)

    def get_lookup(self, name, errorClass=ValueError):
        if name:
            bits = name.split(JSPLITTER)
            fname = bits.pop(0)
            field = self.relmodel._meta.dfields.get(fname)
            meta = self.relmodel._meta
            if field:   # it is a field
                nested = [(self.attname, meta)]
                remaining = JSPLITTER.join(bits)
                name, _nested = field.get_lookup(remaining, errorClass)
                if _nested:
                    nested.extend(_nested)
                return (name, nested)
            else:
                raise errorClass('%s not a valid field for %s' % (fname, meta))
        else:
            return super(ForeignKey, self).get_lookup(name, errorClass)


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

        class MyModel(odm.Model):
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
    type = 'json object'
    internal_type = 'serialized'
    _default = {}

    def _handle_extras(self, as_string=True, **extras):
        self.as_string = as_string
        if not self.as_string and not isinstance(self._default, dict):
            self._default = {}
        self.error_extras(extras)

    def to_python(self, value, backend=None):
        if value is None:
            return self.get_default()
        try:
            return self.encoder.loads(value)
        except TypeError:
            return value

    def set_get_value(self, instance, value):
        # Optimisation, avoid to call serialise since it is the same
        # as to_python
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        if self.as_string:
            # dump as a string
            return self.serialise(value)
        else:
            # unwind as a dictionary
            value = dict(dict_flat_generator(value,
                                             attname=self.attname,
                                             dumps=self.serialise,
                                             error=FieldValueError))
            # If the dictionary is empty we modify so that
            # an update is possible.
            if not value:
                value = {self.attname: self.serialise(None)}
            elif value.get(self.attname, None) is None:
                # TODO Better implementation of this is a ack!
                # set the root value to an empty string to distinguish
                # from None.
                value[self.attname] = self.serialise('')
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

    def get_value(self, instance, *bits):
        value = getattr(instance, self.name)
        try:
            for bit in bits:
                value = value[bit]
            if isinstance(value, dict) and '' in value:
                value = value['']
            return value
        except Exception:
            raise AttributeError


class ModelField(SymbolField):
    '''A filed which can be used to store the model classes (not only
:class:`.Model` models). If a class has a attribute ``_meta``
with a unique hash attribute ``hash`` and it is
registered in the model hash table, it can be used.'''
    type = 'model'
    internal_type = 'text'

    def to_python(self, value, backend=None):
        if value and not hasattr(value, '_meta'):
            value = self.encoder.loads(value)
            return get_model_from_hash(value)
        else:
            return value

    def serialise(self, value, lookup=None):
        if value is not None:
            v = get_hash_from_model(value)
            if v is None:
                value = self.to_python(value)
                return get_hash_from_model(value)
            else:
                return v
    json_serialise = serialise

    def set_get_value(self, instance, value):
        # Optimisation, avoid to call serialise since it is the same
        # as to_python
        value = self.to_python(value)
        setattr(instance, self.attname, value)
        return self.serialise(value)


class ManyToManyField(Field):
    '''A :ref:`many-to-many <many-to-many>` relationship.
Like :class:`ForeignKey`, it requires a positional argument, the class
to which the model is related and it accepts **related_name** as extra
argument.

.. attribute:: related_name

    Optional name to use for the relation from the related object
    back to ``self``. For example::

        class Group(odm.Model):
            name = odm.SymbolField(unique=True)

        class User(odm.Model):
            name = odm.SymbolField(unique=True)
            groups = odm.ManyToManyField(Group, related_name='users')

    To use it::

        >>> g = Group(name='developers').save()
        >>> g.users.add(User(name='john').save())
        >>> u.users.add(User(name='mark').save())

    and to remove::

        >>> u.following.remove(User.objects.get(name='john'))

.. attribute:: through

    An optional :class:`.Model` to use for creating the many-to-many
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
            related.load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        if not self.related_name:
            self.related_name = '%s_set' % self.model._meta.name
        related.Many2ManyThroughModel(self)

    def get_attname(self):
        return None

    def todelete(self):
        return False

    def add_to_fields(self):
        #A many to many field is a dummy field. All it does it provides a proxy
        #for the through model. Remove it from the fields dictionary
        #and addit to the list of many_to_many
        self.meta.dfields.pop(self.name)
        self.meta.manytomany.append(self.name)


class CompositeIdField(AutoIdField):
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
            raise FieldException('At least tow fields are required by '
                                 'composite CompositeIdField')

    def get_value(self, instance, *bits):
        if bits:
            raise AttributeError
        values = tuple((getattr(instance, f.attname) for f in self.fields))
        return hash(values)

    def register_with_model(self, name, model):
        fields = []
        for field in self.fields:
            if field not in model._meta.dfields:
                raise FieldException(
                    'Composite id field "%s" in in "%s" model.' %
                    (field, model._meta))
            field = model._meta.dfields[field]
            if field.internal_type not in ('text', 'numeric'):
                raise FieldException('Composite id field "%s" not valid type.'
                                     % field)
            fields.append(field)
        self.fields = tuple(fields)
        return super(CompositeIdField, self).register_with_model(name, model)
