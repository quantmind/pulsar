from pulsar.apps.test import unittest

from . import local

try:
    import stdnet
except:
    stdnet = None
    

@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class pubsubTest(local.pubsubTest):
    
    @classmethod
    def backend(cls):
        return cls.cfg.redis_server 