import os
import sys
import glob

try:
    from importlib import *
except ImportError:
    from pulsar.utils.fallbacks._importlib import *


def expand_star(mod_name):
    """Expand something like 'unuk.tasks.*' into a list of all the modules
    there.
    """
    expanded = []
    mod_dir  = os.path.dirname(__import__(mod_name[:-2], {}, {}, ['']).__file__)
    for f in glob.glob1(mod_dir, "[!_]*.py"):
        expanded.append('%s.%s' % (mod_name[:-2], f[:-3]))
    return expanded


def import_modules(modules):
    '''Safely import a list of *modules*
    '''
    for mname in modules:
        if mname.endswith('.*'):
            to_load = expand_star(mname)
        else:
            to_load = [mname]
        for module in to_load:
            try:
                __import__(module)
            except ImportError as e:
                pass
            

def module_attribute(dotpath, default = None):
    '''
    Load an attribute from a module.
    If the module or the attriubute is not available,
    return the default argument
    '''
    if dotpath:
        bits = str(dotpath).split('.')
        try:
            module = import_module('.'.join(bits[:-1]))
            return getattr(module,bits[-1],default)
        except Exception as e:
            return default
    else:
        return default


def import_app(module):
    parts = module.rsplit(":", 1)
    if len(parts) == 1:
        module, obj = module, "application"
    else:
        module, obj = parts[0], parts[1]

    try:
        __import__(module)
    except ImportError:
        if module.endswith(".py") and os.path.exists(module):
            raise ImportError("Failed to find application, did "
                "you mean '%s:%s'?" % (module.rsplit(".",1)[0], obj))
        else:
            raise

    mod = sys.modules[module]
    app = eval(obj, mod.__dict__)
    if app is None:
        raise ImportError("Failed to find application object: %r" % obj)
    if not hasattr(app,'__call__'):
        raise TypeError("Application object must be callable.")
    return app