import sys
from inspect import isclass
from copy import copy
from collections import OrderedDict

from pulsar import ImproperlyConfigured
from pulsar.utils.structure import Hash

from .fields import Field, AutoIdField
from .related import class_prepared
from .utils import hashmodel, orderinginfo


def get_fields(bases, attrs):
    #
    fields = []
    for name, field in list(attrs.items()):
        if isinstance(field, Field):
            fields.append((name, attrs.pop(name)))
    #
    fields = sorted(fields, key=lambda x: x[1].creation_counter)
    #
    for base in bases:
        if hasattr(base, '_meta'):
            fields = list((name, copy(field)) for name, field
                          in base._meta.dfields.items()) + fields
    #
    return OrderedDict(fields)


def make_app_label(new_class, app_label=None):
    if app_label is None:
        model_module = sys.modules[new_class.__module__]
        try:
            bits = model_module.__name__.split('.')
            app_label = bits.pop()
            if app_label == 'models':
                app_label = bits.pop()
        except:
            app_label = ''
    return app_label


class ModelMeta(object):
    '''A class for storing meta data for a :class:`.Model` class.
To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`.Model` in the following way::

    from datetime import datetime
    from stdnet import odm

    class MyModel(odm.StdModel):
        timestamp = odm.DateTimeField(default = datetime.now)
        ...

        class Meta:
            ordering = '-timestamp'
            name = 'custom'


:parameter register: if ``True`` (default), this :class:`ModelMeta` is
    registered in the global models hashtable.
:parameter abstract: Check the :attr:`abstract` attribute.
:parameter ordering: Check the :attr:`ordering` attribute.
:parameter app_label: Check the :attr:`app_label` attribute.
:parameter name: Check the :attr:`name` attribute.
:parameter modelkey: Check the :attr:`modelkey` attribute.
:parameter attributes: Check the :attr:`attributes` attribute.

This is the list of attributes and methods available. All attributes,
but the ones mantioned above, are initialized by the object relational
mapper.

.. attribute:: abstract

    If ``True``, This is an abstract Meta class.

.. attribute:: model

    :class:`Model` for which this class is the database metadata container.

.. attribute:: name

    Usually it is the :class:`Model` class name in lower-case, but it
    can be customised.

.. attribute:: app_label

    Unless specified it is the name of the directory or file
    (if at top level) containing the :class:`Model` definition. It can be
    customised.

.. attribute:: modelkey

    The modelkey which is by default given by ``app_label.name``.

.. attribute:: ordering

    Optional name of a :class:`Field` in the :attr:`model`.
    If provided, model indices will be sorted with respect to the value of the
    specified field. It can also be a :class:`autoincrement` instance.
    Check the :ref:`sorting <sorting>` documentation for more details.

    Default: ``None``.

.. attribute:: dfields

    dictionary of :class:`Field` instances.

.. attribute:: fields

    list of all :class:`Field` instances.

.. attribute:: scalarfields

    Ordered list of all :class:`Field` which are not :class:`.StructureField`.
    The order is the same as in the :class:`Model` definition. The :attr:`pk`
    field is not included.

.. attribute:: indices

    List of :class:`Field` which are indices (:attr:`Field.index` attribute
    set to ``True``).

.. attribute:: pk

    The :class:`Field` representing the primary key.

.. attribute:: related

    Dictionary of :class:`related.RelatedManager` for the :attr:`model`. It is
    created at runtime by the object data mapper.

.. attribute:: manytomany

    List of :class:`ManyToManyField` names for the :attr:`model`. This
    information is useful during registration.

.. attribute:: attributes

    Additional attributes for :attr:`model`.
'''
    def __init__(self, model, fields, app_label=None, modelkey=None,
                 name=None, register=True, pkname=None, ordering=None,
                 abstract=False, **kwargs):
        self.model = model
        self.abstract = abstract
        self.dfields = {}
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.related = {}
        self.manytomany = []
        self.model._meta = self
        self.app_label = make_app_label(model, app_label)
        self.name = (name or model.__name__).lower()
        if not modelkey:
            if self.app_label:
                modelkey = '{0}.{1}'.format(self.app_label, self.name)
            else:
                modelkey = self.name
        self.modelkey = modelkey
        if not self.abstract and register:
            hashmodel(model)
        #
        # Check if PK field exists
        pk = None
        pkname = pkname or 'id'
        for name in fields:
            field = fields[name]
            if field.primary_key:
                if pk is not None:
                    raise FieldError("Primary key already available %s."
                                     % name)
                pk = field
                pkname = name
        if pk is None and not self.abstract:
            # ID field not available, create one
            pk = AutoIdField(primary_key=True)
        fields.pop(pkname, None)
        for name, field in fields.items():
            field.register_with_model(name, model)
        if pk is not None:
            pk.register_with_model(pkname, model)
        self.ordering = None
        if ordering:
            self.ordering = self.get_sorting(ordering, ImproperlyConfigured)

    @property
    def type(self):
        '''Model type, either ``structure`` or ``object``.'''
        return self.model._model_type

    def make_object(self, state=None, backend=None):
        '''Create a new instance of :attr:`model` from a *state* tuple.'''
        model = self.model
        obj = model.__new__(model)
        self.load_state(obj, state, backend)
        return obj

    def load_state(self, obj, state=None, backend=None):
        if state:
            pkvalue, loadedfields, data = state
            pk = self.pk
            pkvalue = pk.to_python(pkvalue, backend)
            setattr(obj, pk.attname, pkvalue)
            if loadedfields is not None:
                loadedfields = tuple(loadedfields)
            obj._loadedfields = loadedfields
            for field in obj.loadedfields():
                value = field.value_from_data(obj, data)
                setattr(obj, field.attname, field.to_python(value, backend))
            if backend or ('__dbdata__' in data and
                           data['__dbdata__'][pk.name] == pkvalue):
                obj.dbdata[pk.name] = pkvalue

    def __repr__(self):
        return self.modelkey

    def __str__(self):
        return self.__repr__()

    def pkname(self):
        '''Primary key name. A shortcut for ``self.pk.name``.'''
        return self.pk.name

    def pk_to_python(self, value, backend):
        '''Convert the primary key ``value`` to a valid python representation.
        '''
        return self.pk.to_python(value, backend)

    def is_valid(self, instance):
        '''Perform validation for *instance* and stores serialized data,
indexes and errors into local cache.
Return ``True`` if the instance is ready to be saved to database.'''
        dbdata = instance.dbdata
        data = dbdata['cleaned_data'] = {}
        errors = dbdata['errors'] = {}
        #Loop over scalar fields first
        for field, value in instance.fieldvalue_pairs():
            name = field.attname
            try:
                svalue = field.set_get_value(instance, value)
            except Exception as e:
                errors[name] = str(e)
            else:
                if (svalue is None or svalue is '') and field.required:
                    errors[name] = ("Field '{0}' is required for '{1}'."
                                    .format(name, self))
                else:
                    if isinstance(svalue, dict):
                        data.update(svalue)
                    elif svalue is not None:
                        data[name] = svalue
        return len(errors) == 0

    def get_sorting(self, sortby, errorClass=None):
        desc = False
        if isinstance(sortby, autoincrement):
            f = self.pk
            return orderinginfo(sortby, f, desc, self.model, None, True)
        elif sortby.startswith('-'):
            desc = True
            sortby = sortby[1:]
        if sortby == self.pkname():
            f = self.pk
            return orderinginfo(f.attname, f, desc, self.model, None, False)
        else:
            if sortby in self.dfields:
                f = self.dfields[sortby]
                return orderinginfo(f.attname, f, desc, self.model,
                                    None, False)
            sortbys = sortby.split(JSPLITTER)
            s0 = sortbys[0]
            if len(sortbys) > 1 and s0 in self.dfields:
                f = self.dfields[s0]
                nested = f.get_sorting(JSPLITTER.join(sortbys[1:]), errorClass)
                if nested:
                    sortby = f.attname
                return orderinginfo(sortby, f, desc, self.model, nested, False)
        errorClass = errorClass or ValueError
        raise errorClass('"%s" cannot order by attribute "%s". It is not a '
                         'scalar field.' % (self, sortby))

    def backend_fields(self, fields):
        '''Return a two elements tuple containing a list
of fields names and a list of field attribute names.'''
        dfields = self.dfields
        processed = set()
        names = []
        atts = []
        pkname = self.pkname()
        for name in fields:
            if name == pkname or name in processed:
                continue
            elif name in dfields:
                processed.add(name)
                field = dfields[name]
                names.append(field.name)
                atts.append(field.attname)
            else:
                bname = name.split(JSPLITTER)[0]
                if bname in dfields:
                    field = dfields[bname]
                    if field.type in ('json object', 'related object'):
                        processed.add(name)
                        names.append(name)
                        atts.append(name)
        return names, atts

    def as_dict(self):
        '''Model metadata in a dictionary'''
        pk = self.pk
        id_type = 3
        if pk.type == 'auto':
            id_type = 1
        return {'id_name': pk.name,
                'id_type': id_type,
                'sorted': bool(self.ordering),
                'autoincr': self.ordering and self.ordering.auto,
                'multi_fields': [field.name for field in self.multifields],
                'indices': dict(((idx.attname, idx.unique)
                                 for idx in self.indices))}


class ModelType(type):
    '''Model metaclass'''
    def __new__(cls, name, bases, attrs):
        meta = attrs.pop('Meta', None)
        if isclass(meta):
            meta = dict(((k, v) for k, v in meta.__dict__.items()
                         if not k.startswith('__')))
        else:
            meta = meta or {}
        cls.extend_meta(meta, attrs)
        fields = get_fields(bases, attrs)
        new_class = super(ModelType, cls).__new__(cls, name, bases, attrs)
        ModelMeta(new_class, fields, **meta)
        class_prepared.fire(new_class)
        return new_class

    @classmethod
    def extend_meta(cls, meta, attrs):
        for name in ('register', 'abstract', 'attributes'):
            if name in attrs:
                meta[name] = attrs.pop(name)


class Model(ModelType('ModelBase', (Hash,), {'abstract': True})):
    '''A :class:`Model` which contains data in :class:`.Field`.'''
    abstract = True

    @classmethod
    def create_model(cls, name, **fields):
        return ModelType(name, (cls,), fields)


class autoincrement(object):
    '''An :class:`autoincrement` is used in a :class:`StdModel` Meta
class to specify a model with :ref:`incremental sorting <incremental-sorting>`.

.. attribute:: incrby

    The amount to increment the score by when a duplicate element is saved.

    Default: 1.

For example, the :class:`stdnet.apps.searchengine.Word` model is defined as::

    class Word(odm.StdModel):
        id = odm.SymbolField(primary_key = True)

        class Meta:
            ordering = -autoincrement()

This means every time we save a new instance of Word, and that instance has
an id already available, the score of that word is incremented by the
:attr:`incrby` attribute.

'''
    def __init__(self, incrby=1, desc=False):
        self.incrby = incrby
        self._asce = -1 if desc else 1

    def __neg__(self):
        c = copy(self)
        c._asce *= -1
        return c

    @property
    def desc(self):
        return True if self._asce == -1 else False

    def __repr__(self):
        return ('' if self._asce == 1 else '-'
                ) + '{0}({1})'.format(self.__class__.__name__, self.incrby)

    def __str__(self):
        return self.__repr__()

