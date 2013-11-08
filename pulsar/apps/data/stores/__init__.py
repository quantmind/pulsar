from pulsar.utils.importer import import_module

for name in ['redis']:
    try:
        import_module('pulsar.apps.data.stores.%s' % name)
    except ImportError:
        pass
