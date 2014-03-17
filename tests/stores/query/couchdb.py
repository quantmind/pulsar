import unittest

from . import QueryTest
from ..couchdb import CouchDbTest


class TestCouchdbQuery(CouchDbTest, QueryTest, unittest.TestCase):
    pass
