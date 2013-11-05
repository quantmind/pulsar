from pulsar.utils.importer import import_module

import_module('pulsar.apps.data.stores.pulsar')


for name in ['redis']:
    try:
        import_module('pulsar.apps.data.stores.%s' % name)
    except ImportError:
        pass
