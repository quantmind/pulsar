import unittest

from . import Odm
from ..couchdb import CouchDbTest


class TestCouchdbODM(CouchDbTest, Odm, unittest.TestCase):
    pass
