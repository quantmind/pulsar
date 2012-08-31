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
            
def module_attribute(dotpath, default=None, safe=False):
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
            if not safe:
                raise
            return default
    else:
        if not safe:
            raise ImportError()
        return default

def py_file(name):
    if name.endswith('.py'):
        return name[:-3]
    elif name.endswith('.pyc'):
        return name[:-4]
    else:
        return name
    
def import_system_file(mod, add_to_path=True):
    if os.path.isfile(mod):
        # it is a file in the system path
        dir, name = os.path.split(mod)
        names = [py_file(name)]
        while dir and not dir in sys.path:
            dir, name = os.path.split(dir)
            names.insert(0, name)
        # the file was not in the system path
        if not dir and add_to_path:
            dir, name = os.path.split(mod)
            if dir:
                sys.path.append(dir)
            mod_name = py_file(name)
        else:
            mod_name = '.'.join(names)
        return import_module(mod_name)
