import sys
from inspect import isclass
from copy import copy
from base64 import b64encode
from collections import Mapping

from pulsar import ImproperlyConfigured
from pulsar.utils.structures import OrderedDict
from pulsar.utils.pep import ispy3k, iteritems

try:
    from pulsar.utils.libs import Model as CModelBase
except ImportError:
    CModelBase = None

from .manager import class_prepared, makeManyToManyRelatedManager
from .fields import (Field, AutoIdField, ForeignKey, CompositeIdField,
                     FieldError, NONE_EMPTY)
from .query import OdmError

from ..store import Command


primary_keys = ('id', 'ID', 'pk', 'PK')


def is_private_field(field):
    return field.startswith('_') or field == 'Type'


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
    To override default behaviour you can specify the ``Meta`` class
    as an inner class of :class:`.Model` in the following way::

        from pulsar.apps.data import odm

        class MyModel(odm.Model):
            timestamp = odm.FloatField()
            ...

            class Meta:
                name = 'custom'


    :parameter register: if ``True`` (default), this :class:`ModelMeta` is
        registered in the global models hashtable.
    :parameter abstract: Check the :attr:`abstract` attribute.
    :parameter app_label: Check the :attr:`app_label` attribute.
    :parameter name: Check the :attr:`name` attribute.
    :parameter table_name: Check the :attr:`table_name` attribute

    This is the list of attributes and methods available. All attributes,
    but the ones mentioned above, are initialised by the object relational
    mapper.

    .. attribute:: abstract

        If ``True``, This is an abstract Meta class.

    .. attribute:: model

        :class:`Model` for which this instance is the database metadata
        container.

    .. attribute:: name

        Usually it is the :class:`.Model` class name in lower-case, but it
        can be customised.

    .. attribute:: app_label

        Unless specified it is the name of the directory or file
        (if at top level) containing the :class:`.Model` definition.
        It can be customised.

    .. attribute:: table_name

        The table_name which is by default given by ``<app_label>_<name>``.

    .. attribute:: dfields

        dictionary of all :class:`.Field` in :attr:`model`

    .. attribute:: scalarfields

        Ordered list of all :class:`Field` which are not
        :class:`.StructureField`.
        The order is the same as in the :class:`Model` definition.

    .. attribute:: indexes

        List of :class:`.Field` which are indexes (:attr:`~.Field.index`
        attribute set to ``True``).

    .. attribute:: pk

        The :class:`.Field` representing the primary key.

    .. attribute:: related

        Dictionary of :class:`.RelatedManager` for the :attr:`model`.
        It is created at runtime by the object data mapper.

    .. attribute:: manytomany

        List of :class:`ManyToManyField` names for the :attr:`model`. This
        information is useful during registration.
    '''
    def __init__(self, model, fields, app_label=None, table_name=None,
                 name=None, register=True, pkname=None, abstract=False,
                 **kwargs):
        self.model = model
        self.abstract = abstract
        self.scalarfields = []
        self.manytomany = []
        self.indexes = []
        self.manytomany = []
        self.dfields = {}
        self.converters = {}
        self.related = {}
        self.model._meta = self
        self.app_label = make_app_label(model, app_label)
        self.name = (name or model.__name__).lower()
        if not table_name:
            if self.app_label:
                table_name = '%s_%s' % (self.app_label, self.name)
            else:
                table_name = self.name
        self.table_name = table_name
        #
        # Check if PK field exists
        pk = None
        pkname = pkname or primary_keys[0]
        scalarfields = []
        for name in fields:
            field = fields[name]
            if is_private_field(name):
                raise FieldError("%s is a reserved field name" % name)
            if field.primary_key:
                if pk is not None:
                    raise FieldError("Primary key already available %s."
                                     % name)
                pk = field
                pkname = name
            elif name in primary_keys:
                raise FieldError('%s is a reserved field name for primary keys'
                                 % name)
        if pk is None and not self.abstract:
            # ID field not available, create one
            pk = AutoIdField()
        if not self.abstract:
            fields.pop(pkname, None)
            for name, field in fields.items():
                field.register_with_model(name, model)
            pk.register_with_model(pkname, model)

    def __repr__(self):
        return self.table_name

    def __str__(self):
        return self.__repr__()

    @property
    def _meta(self):
        return self

    def pkname(self):
        '''Primary key name. A shortcut for ``self.pk.name``.'''
        return self.pk.name

    def pk_to_python(self, value, backend):
        '''Convert the primary key ``value`` to a valid python representation.
        '''
        return self.pk.to_python(value, backend)

    def store_data(self, instance, store, action):
        '''Generator of ``field, value`` pair for the data ``store``.

        Perform validation for ``instance`` and can raise :class:`.FieldError`
        if invalid values are stored in ``instance``.
        '''
        fields = instance._meta.dfields
        if action == Command.INSERT:
            for field in fields.values():
                name = field.store_name
                value = field.to_store(instance.get_raw(name), store)
                if ((value in NONE_EMPTY) and field.required and
                        not isinstance(field, AutoIdField)):
                    raise FieldError("Field '%s' is required for '%s'." %
                                     (name, self))
                if value is not None:
                    yield name, value
            for name in (set(instance) - set(fields)):
                if not is_private_field(name):
                    value = instance[name]
                    if value is not None:
                        yield name, value
        else:
            for name in instance:
                if not is_private_field(name):
                    value = instance[name]
                    if name in fields:
                        value = fields[name].to_store(value, store)
                    if value is not None:
                        yield name, value


class ModelType(type(dict)):
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
        attrs['__slots__'] = ('_access_cache', '_modified')
        new_class = super(ModelType, cls).__new__(cls, name, bases, attrs)
        ModelMeta(new_class, fields, **meta)
        class_prepared.fire(new_class)
        return new_class

    @classmethod
    def extend_meta(cls, meta, attrs):
        for name in ('register', 'abstract'):
            if name in attrs:
                meta[name] = attrs.pop(name)


class Model(ModelType('ModelBase', (dict,), {'abstract': True})):
    '''A model is a python ``dict`` which represents and item/row
    in a data-store collection/table.

    Fields values can be accessed via the dictionary interface::

        model['field1']

    or the dotted interface::

        model.field1

    which is equivalent to::

        model.get('field1')

    .. attribute:: _meta

        Class attribute which represents the :class:`.ModelMeta`
        for this model.
    '''
    abstract = True

    def __init__(self, *args, **kwargs):
        self._access_cache = set()
        self._modified = None
        self.update(*args, **kwargs)
        self._modified = set()

    def __getitem__(self, field):
        field = mstr(field)
        if field in primary_keys:
            field = self._meta.pkname()
        value = super(Model, self).__getitem__(field)
        if field not in self._access_cache:
            self._access_cache.add(field)
            if field in self._meta.converters:
                value = self._meta.converters[field](value)
                super(Model, self).__setitem__(field, value)
        return value

    def __setitem__(self, field, value):
        field, value = self._get_field_value(field, value)
        super(Model, self).__setitem__(field, value)

    def __getattr__(self, field):
        try:
            return self.__getitem__(field)
        except KeyError as e:
            return None

    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.id == other.id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get(self, field, default=None):
        try:
            return self.__getitem__(field)
        except KeyError:
            return default

    def set(self, field, value, modify=True):
        '''Set the ``value`` at ``field``

        If ``modify`` is ``True``, this method is equivalent to::

            model[field] = value
        '''
        if modify:
            self[field] = value
        else:
            super(Model, self).__setitem__(field, value)

    def get_raw(self, field, default=None):
        '''Get the raw value at ``field``

        This function does not apply field conversion.
        '''
        try:
            return super(Model, self).__getitem__(field)
        except KeyError:
            return default

    def update(self, *args, **kwargs):
        if len(args) == 1:
            iterable = args[0]
            super(Model, self).update(self._update_modify(iterable))
        elif args:
            raise TypeError('expected at most 1 arguments, got %s' % len(args))
        if kwargs:
            super(Model, self).update(self._update_modify(kwargs))

    def to_json(self):
        '''Return a JSON serialisable dictionary representation.
        '''
        return dict(self._to_json())

    def save(self):
        '''Commit changes to backend data store.
        '''
        mapper = self.get('_mapper')
        if mapper:
            return mapper[self._meta].save(self)
        else:
            raise OdmError('_mapper not available in %s' % self)

    def delete(self):
        '''Delete this model from backend data store
        '''
        mapper = self.get('_mapper')
        if mapper:
            return mapper[self._meta].delete(self)
        else:
            raise OdmError('_mapper not available in %s' % self)

    #    INTERNALS
    def _to_json(self):
        pk = self.get('id')
        if pk:
            yield self._meta.pk.name, pk
            for key in self:
                if not is_private_field(key):
                    value = self[key]
                    if value is not None:
                        if key in self._meta.dfields:
                            value = self._meta.dfields[key].to_json(value)
                        elif isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8')
                            except Exception:
                                value = b64encode(value).decode('utf-8')
                        yield key, value

    def _update_modify(self, iterable):
        if isinstance(iterable, Mapping):
            iterable = iteritems(iterable)
        for field, value in iterable:
            yield self._get_field_value(field, value)

    def _get_field_value(self, field, value):
        field = mstr(field)
        if not is_private_field(field):
            if field in primary_keys:
                field = self._meta.pkname()
            if field in self._meta.dfields:
                f = self._meta.dfields[field]
                value = f.get_value(self, value)
                field = f.store_name
            if self._modified is not None:
                self._access_cache.discard(field)
                if field in self:
                    if super(Model, self).__getitem__(field) != value:
                        self._modified.add(field)
                else:
                    self._modified.add(field)
        return field, value

    @classmethod
    def _many2many_through_model(cls, field):
        field.relmodel = cls
        if not field.related_name:
            field.related_name = '%s_set' % field.model._meta.name
        name_model = field.model._meta.name
        name_relmodel = field.relmodel._meta.name
        # The two models are the same.
        if name_model == name_relmodel:
            name_relmodel += '2'
        through = field.through
        # Create the through model
        if through is None:
            name = '{0}_{1}'.format(name_model, name_relmodel)

            class Meta:
                app_label = field.model._meta.app_label
            through = ModelType(name, (StdModel,), {'Meta': Meta})
            field.through = through
        # The first field
        field1 = ForeignKey(field.model,
                            related_name=field.name,
                            related_manager_class=makeManyToManyRelatedManager(
                                field.relmodel,
                                name_model,
                                name_relmodel)
                            )
        field1.register_with_model(name_model, through)
        # The second field
        field2 = ForeignKey(field.relmodel,
                            related_name=field.related_name,
                            related_manager_class=makeManyToManyRelatedManager(
                                field.model,
                                name_relmodel,
                                name_model)
                            )
        field2.register_with_model(name_relmodel, through)
        pk = CompositeIdField(name_model, name_relmodel)
        pk.register_with_model('id', through)


PyModel = Model


def create_model(name, **params):
    '''Create a :class:`.Model` class.

    :param name: Name of the model class.
    :param params: key-valued parameter to pass to the :class:`ModelMeta`
        constructor.
    :return: a local :class:`Model` class.
    '''
    return ModelType(name, (Model,), params)


if ispy3k:
    def mstr(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s

else:
    def mstr(s):
        if isinstance(s, (bytes, unicode)):
            return s
        else:
            return str(s)
