__test__ = False

from pulsar.apps.data import odm


class User(odm.Model):
    username = odm.CharField(unique=True)
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField()
    last_name = odm.CharField()
    email = odm.CharField(unique=True)
    is_active = odm.BooleanField(default=True)
    can_login = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)
    data = odm.JSONField()
