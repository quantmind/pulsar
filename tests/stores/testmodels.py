__test__ = False

from pulsar.apps.data import odm


class User(odm.Model):
    username = odm.CharField(unique=True)
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField(index=True)
    last_name = odm.CharField(index=True)
    email = odm.CharField(unique=True)
    is_active = odm.BooleanField(index=True, default=True)
    can_login = odm.BooleanField(index=True, default=True)
    is_superuser = odm.BooleanField(index=True, default=False)
    data = odm.JSONField()


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
