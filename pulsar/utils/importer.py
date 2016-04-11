import os
import sys
import glob
from importlib import import_module

import importlib.util

from .slugify import slugify


def _import_system_file(filename):
    module_name = slugify(filename[:-3], '.')
    spec = importlib.util.spec_from_file_location(module_name, filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def expand_star(mod_name):
    """Expand something like 'unuk.tasks.*' into a list of all the modules
    there.
    """
    expanded = []
    mod_dir = os.path.dirname(
        __import__(mod_name[:-2], {}, {}, ['']).__file__)
    for f in glob.glob1(mod_dir, "[!_]*.py"):
        expanded.append('%s.%s' % (mod_name[:-2], f[:-3]))
    return expanded


def import_modules(modules, safe=True):
    '''Safely import a list of *modules*
    '''
    all = []
    for mname in modules:
        if mname.endswith('.*'):
            to_load = expand_star(mname)
        else:
            to_load = [mname]
        for module in to_load:
            try:
                all.append(import_module(module))
            except ImportError:
                if not safe:
                    raise
    return all


def module_attribute(dotpath, default=None, safe=False):
    '''Load an attribute from a module.

    If the module or the attribute is not available, return the default
    argument if *safe* is `True`.
    '''
    if dotpath:
        bits = str(dotpath).split(':')
        try:
            if len(bits) == 2:
                attr = bits[1]
                module_name = bits[0]
            else:
                bits = bits[0].split('.')
                if len(bits) > 1:
                    attr = bits[-1]
                    module_name = '.'.join(bits[:-1])
                else:
                    raise ValueError('Could not find attribute in %s'
                                     % dotpath)

            module = import_module(module_name)
            return getattr(module, attr)
        except Exception:
            if not safe:
                raise
            return default
    else:
        if not safe:
            raise ImportError
        return default


py_extensions = set(('py', 'pyc', 'pyd', 'pyo'))


def py_file(name):
    bits = name.split('.')
    if len(bits) == 2 and bits[1] in py_extensions:
        return bits[0]
    elif len(bits) == 1:
        return name


def import_system_file(mod, safe=True):
    if os.path.isfile(mod):
        return _import_system_file(mod)
    else:
        try:
            return import_module(mod)
        except ImportError:
            mod2 = os.path.join(mod, '__init__.py')
            if os.path.isfile(mod2):
                return _import_system_file(mod2)
            elif not safe:
                raise
            pass
