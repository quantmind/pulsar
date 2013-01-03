import unittest

import pulsar
from pulsar.utils.httpurl import iteritems, itervalues
from pulsar.apps.test.result import Plugin


__all__ = ['WrapTest',
           'TestOption',
           'TestPlugin']

class TestOption(pulsar.Setting):
    virtual = True
    app = 'test'
    section = "Test"


class WrapTest(object):
    '''Wrap an underlying test case'''
    def __init__(self, test):
        self.test = test
        setattr(self, test._testMethodName, self._call)
        self.testMethod = getattr(test, test._testMethodName)

    def __str__(self):
        return self.test._testMethodName
    __repr__ = __str__
    
    @property
    def original_test(self):
        if isinstance(self.test, WrapTest):
            return self.test.original_test
        else:
            return self.test
        
    def set_test_attribute(self, name, value):
        setattr(self.original_test, name, value)
        
    def __getattr__(self, name):
        return getattr(self.original_test, name)
        
    def _call(self):
        # This is the actual function to implement
        return self.testMethod()
    

def as_test_setting(setting):
    setting.app = 'test-plugin'
    setting.section = "Test"
    return setting
    
class TestPluginMeta(type):
    
    def __new__(cls, name, bases, attrs):
        settings = {}
        for base in bases:
            if isinstance(base, TestPluginMeta):
                settings.update(base.config.settings)
        for key, setting in list(iteritems(attrs)):
            if isinstance(setting, pulsar.Setting):
                attrs.pop(key)
                setting.name = setting.name or key.lower()
                settings[setting.name] = as_test_setting(setting)
        if not attrs.pop('virtual', False):
            setting_name = attrs.pop('name', name).lower()
            if setting_name:
                def_flag = '--%s' % setting_name.replace(' ','-').replace('_','-')
                action = attrs.pop('action', None)
                type = attrs.pop('type', None)
                default = attrs.pop('default', None)
                validator = attrs.pop('validator', None)
                nargs = attrs.pop('nargs', None)
                if validator is None and default is None and type is None\
                    and nargs is None:
                    if action is None or action == 'store_true':
                        action = 'store_true'
                        default = False
                        validator = pulsar.validate_bool
                    elif action == 'store_false':
                        default = True
                        validator = pulsar.validate_bool
                setting = pulsar.Setting(name=setting_name,
                                    desc=attrs.pop('desc', name),
                                    type=type,
                                    flags=attrs.pop('flags', [def_flag]),
                                    action=action,
                                    default=default,
                                    validator=validator,
                                    nargs=nargs)
                settings[setting.name] = as_test_setting(setting)
        attrs['config'] = pulsar.Config(settings=settings)
        return super(TestPluginMeta, cls).__new__(cls, name, bases, attrs)
                
    
class TestPlugin(TestPluginMeta('TestPluginBase',(Plugin,),{'virtual': True})):
    '''Base class for plugins which can be added to a :class:`TestSuite`
to extend its functionalities. :class:`pulsar.Setting` are specified as
class attributes and collected into the :attr:`config` attribute.

If the :attr:`name` is not specified or its value validate as True,
an additional settings is added to the configuration.
'''
    virtual = True
    
    def __new__(cls):
        o = super(TestPlugin, cls).__new__(cls)
        o.config = cls.config.copy()
        return o
        
    def configure(self, cfg):
        self.config = cfg
        
    def include_settings(self):
        return [s.name for settinng in self.settings]
    