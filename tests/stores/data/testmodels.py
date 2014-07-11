__test__ = False
from datetime import datetime, timedelta

from pulsar.apps.data import odm


default_expiry = lambda: datetime.now() + timedelta(days=7)


class User(odm.Model):
    username = odm.CharField(unique=True)
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField(required=False, index=True)
    last_name = odm.CharField(required=False, index=True)
    email = odm.CharField(required=False, unique=True)
    is_active = odm.BooleanField(default=True)
    can_login = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)
    data = odm.JSONField()


class Session(odm.Model):
    '''A session model with a hash table as data store.'''
    data = odm.JSONField()
    expiry = odm.DateTimeField(default=default_expiry)
    user = odm.ForeignKey(User)


class Blog(odm.Model):
    published = odm.DateField()
    title = odm.CharField()
    body = odm.CharField()


class StoreTest(object):

    @classmethod
    def name(cls, name):
        '''A modified name with the execution id
        '''
        return ('test_%s_%s' % (cls.cfg.exc_id, name)).lower()

    @classmethod
    def mapper(cls, *models, **kw):
        '''Create a mapper for models'''
        mapper = odm.Mapper(cls.store)
        for model in models:
            mapper.register(model)
        return mapper

    @classmethod
    def create_store(cls, **kwargs):
        raise NotImplementedError
