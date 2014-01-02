from functools import partial

from .model import class_prepared, create_model
from .mapper import Manager, LazyProxy
from .fields import FieldError, Field, CompositeIdField


RECURSIVE_RELATIONSHIP_CONSTANT = 'self'

pending_lookups = {}


def load_relmodel(field, callback):
    relmodel = None
    relation = field.relmodel
    if relation == RECURSIVE_RELATIONSHIP_CONSTANT:
        relmodel = field.model
    else:
        try:
            app_label, model_name = relation.lower().split(".")
        except ValueError:
            # If we can't split, assume a model in current app
            app_label = field.model._meta.app_label
            model_name = relation.lower()
        except AttributeError:
            relmodel = relation
    if relmodel:
        callback(relmodel)
    else:
        key = '%s.%s' % (app_label, model_name)
        if key not in pending_lookups:
            pending_lookups[key] = []
        pending_lookups[key].append(callback)


def do_pending_lookups(model, **kwargs):
    """Handle any pending relations to the sending model.
Sent from class_prepared."""
    key = model._meta.table_name
    for callback in pending_lookups.pop(key, []):
        callback(model)


class_prepared.bind(do_pending_lookups)


def Many2ManyThroughModel(field):
    '''Create a Many2Many through model with two foreign key fields and a
    CompositeFieldId depending on the two foreign keys.
    '''
    name_model = field.model._meta.name
    name_relmodel = field.relmodel._meta.name
    # The two models are the same.
    if name_model == name_relmodel:
        name_relmodel += '2'
    through = field.through
    # Create the through model
    if through is None:
        name = '{0}_{1}'.format(name_model, name_relmodel)
        through = create_model(name,
                               meta={'app_label': field.model._meta.app_label})
        field.through = through
    # The first field
    field1 = ForeignKey(field.model,
                        related_name=field.name,
                        related_manager_class=makeMany2ManyRelatedManager(
                            field.relmodel,
                            name_model,
                            name_relmodel)
                        )
    field1.register_with_model(name_model, through)
    # The second field
    field2 = ForeignKey(field.relmodel,
                        related_name=field.related_name,
                        related_manager_class=makeMany2ManyRelatedManager(
                            field.model,
                            name_relmodel,
                            name_model)
                        )
    field2.register_with_model(name_relmodel, through)
    pk = CompositeIdField(name_model, name_relmodel)
    pk.register_with_model('id', through)


class LazyForeignKey(LazyProxy):
    '''Descriptor for a :class:`ForeignKey` field.'''
    def load(self, instance, session=None, backend=None):
        return instance._load_related_model(self.field)

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("%s must be accessed via instance" %
                                 self._field.name)
        field = self.field
        if value is not None and not isinstance(value, field.relmodel):
            raise ValueError(
                'Cannot assign "%r": "%s" must be a "%s" instance.' %
                (value, field, field.relmodel._meta.name))

        cache_name = self.field.get_cache_name()
        # If we're setting the value of a OneToOneField to None,
        # we need to clear
        # out the cache on any old related object. Otherwise, deleting the
        # previously-related object will also cause this object to be deleted,
        # which is wrong.
        if value is None:
            # Look up the previously-related object, which may still
            # be available since we've not yet cleared out the related field.
            related = getattr(instance, cache_name, None)
            if related:
                try:
                    delattr(instance, cache_name)
                except AttributeError:
                    pass
            setattr(instance, self.field.attname, None)
        else:
            setattr(instance, self.field.attname, value.pkvalue())
            setattr(instance, cache_name, value)


class RelatedManager(Manager):
    '''Base class for managers handling relationships between models.
While standard :class:`Manager` are class properties of a model,
related managers are accessed by instances to easily retrieve instances
of a related model.

.. attribute:: relmodel

    The :class:`.Model` this related manager relates to.

.. attribute:: related_instance

    An instance of the :attr:`relmodel`.
'''
    def __init__(self, field, model=None, instance=None):
        self.field = field
        model = model or field.model
        super(RelatedManager, self).__init__(model)
        self.related_instance = instance

    def __get__(self, instance, instance_type=None):
        return self.__class__(self.field, self.model, instance)


class One2ManyRelatedManager(RelatedManager):
    '''A specialised :class:`RelatedManager` for handling one-to-many
relationships under the hood.
If a model has a :class:`ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    @property
    def relmodel(self):
        return self.field.relmodel

    def query(self, session=None):
        # Override query method to account for related instance if available
        query = super(One2ManyRelatedManager, self).query(session)
        if self.related_instance is not None:
            kwargs = {self.field.name: self.related_instance}
            return query.filter(**kwargs)
        else:
            return query

    def query_from_query(self, query, params=None):
        if params is None:
            params = query
        return query.session.query(self.model, fargs={self.field.name: params})


class Many2ManyRelatedManager(One2ManyRelatedManager):
    '''A specialised :class:`.Manager` for handling
many-to-many relationships under the hood.
When a model has a :class:`ManyToManyField`, instances
of that model will have access to the related objects via a simple
attribute of the model.'''
    def session_instance(self, name, value, session, **kwargs):
        if self.related_instance is None:
            raise ManyToManyError('Cannot use "%s" method from class' % name)
        elif not self.related_instance.pkvalue():
            raise ManyToManyError('Cannot use "%s" method on a non persistent '
                                  'instance.' % name)
        elif not isinstance(value, self.formodel):
            raise ManyToManyError(
                '%s is not an instance of %s' % (value, self.formodel._meta))
        elif not value.pkvalue():
            raise ManyToManyError('Cannot use "%s" a non persistent instance.'
                                  % name)
        kwargs.update({self.name_formodel: value,
                       self.name_relmodel: self.related_instance})
        return self.session(session), self.model(**kwargs)

    def add(self, value, session=None, **kwargs):
        '''Add ``value``, an instance of :attr:`formodel` to the
:attr:`through` model. This method can only be accessed by an instance of the
model for which this related manager is an attribute.'''
        s, instance = self.session_instance('add', value, session, **kwargs)
        return s.add(instance)

    def remove(self, value, session=None):
        '''Remove *value*, an instance of ``self.model`` from the set of
elements contained by the field.'''
        s, instance = self.session_instance('remove', value, session)
        # update state so that the instance does look persistent
        instance.get_state(iid=instance.pkvalue(), action='update')
        return s.delete(instance)

    def throughquery(self, session=None):
        '''Return a :class:`Query` on the ``throughmodel``, the model
used to hold the :ref:`many-to-many relationship <many-to-many>`.'''
        return super(Many2ManyRelatedManager, self).query(session)

    def query(self, session=None):
        # Return a query for the related model
        ids = self.throughquery(session).get_field(self.name_formodel)
        pkey = self.formodel.pk().name
        fargs = {pkey: ids}
        return self.session(session).query(self.formodel).filter(**fargs)


def makeMany2ManyRelatedManager(formodel, name_relmodel, name_formodel):
    '''formodel is the model which the manager .'''

    class _Many2ManyRelatedManager(Many2ManyRelatedManager):
        pass

    _Many2ManyRelatedManager.formodel = formodel
    _Many2ManyRelatedManager.name_relmodel = name_relmodel
    _Many2ManyRelatedManager.name_formodel = name_formodel
    return _Many2ManyRelatedManager


class ForeignKey(Field):
    '''A field defining a :ref:`one-to-many <one-to-many>` objects
    relationship.
    Requires a positional argument: the class to which the model is related.
    For example::

        class Folder(odm.Model):
            name = odm.CharField()

        class File(odm.Model):
            folder = odm.ForeignKey(Folder, related_name='files')

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
    proxy_class = LazyForeignKey
    related_manager_class = One2ManyRelatedManager

    def __init__(self, model, related_name=None, related_manager_class=None,
                 **kwargs):
        if related_manager_class:
            self.related_manager_class = related_manager_class
        super(ForeignKey, self).__init__(**kwargs)
        if not model:
            raise FieldError('Model not specified')
        self.relmodel = model
        self.related_name = related_name

    def register_with_related_model(self):
        # add the RelatedManager proxy to the model holding the field
        setattr(self.model, self.name, self.proxy_class(self))
        load_relmodel(self, self._set_relmodel)

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
            raise FieldError('Duplicated related name "{0} in model "{1}" '
                             'and field {2}'.format(self.related_name,
                                                    meta, self))

    def _register_with_related_model(self):
        manager = self.related_manager_class(self)
        setattr(self.relmodel, self.related_name, manager)
        self.relmodel._meta.related[self.related_name] = manager
        self.relmodel_manager = manager

    def get_attname(self):
        return '%s_id' % self.name


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
        relationship can be passed to the constructor, via the **through**
        keyword.
        If such a model is not passed, under the hood, a
        :class:`ManyToManyField` creates a new *model* with name constructed
        from the field name and the model holding the field.
        In the example above it would be *group_user*.
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
        self.relmodel = relmodel
        if not self.related_name:
            self.related_name = '%s_set' % self.model._meta.name
        Many2ManyThroughModel(self)

    def get_attname(self):
        return None

    def todelete(self):
        return False

    def add_to_fields(self):
        #A many to many field is a dummy field. All it does it provides a proxy
        #for the through model. Remove it from the fields dictionary
        #and addit to the list of many_to_many
        self._meta.dfields.pop(self.name)
        self._meta.manytomany.append(self.name)
