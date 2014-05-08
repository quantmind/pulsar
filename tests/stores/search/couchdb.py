import unittest

from pulsar.apps.data import odm

from . import SearchTest
from ..couchdb import CouchDbTest


class TestCouchdbSearch(CouchDbTest, SearchTest, unittest.TestCase):

    @classmethod
    def create_search_engine(cls):
        return odm.search_engine(cls.models.default_store.dns)
