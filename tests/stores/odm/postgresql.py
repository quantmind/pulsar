import unittest

from . import Odm
from ..postgresql import PostgreSqlTest


class PostgreSqlTestODM(PostgreSqlTest, Odm, unittest.TestCase):
    pass
