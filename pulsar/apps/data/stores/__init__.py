from pulsar.utils.importer import import_module
from .pulsarstore import *

for name in ['redis']:
    try:
        import_module('pulsar.apps.data.stores.%s' % name)
    except ImportError:
        pass
