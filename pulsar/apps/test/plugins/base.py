import pulsar
from pulsar.utils.importer import module_attribute
from pulsar.apps.test.result import Plugin


__all__ = ['WrapTest', 'TestPlugin']


def as_test_setting(setting):
    setting.app = 'test'
    setting.section = "Test"
    return setting


def validate_plugin_list(val):
    if val and not isinstance(val, (list, tuple)):
        raise TypeError("Not a list: %s" % val)
    values = []
    for v in val:
        values.append(module_attribute(v, safe=False)())
    return values


class WrapTest:
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


class TestPluginMeta(type):

    def __new__(cls, name, bases, attrs):
        settings = {}
        for base in bases:
            if isinstance(base, TestPluginMeta):
                settings.update(base.config.settings)
        for key, setting in list(attrs.items()):
            if isinstance(setting, pulsar.Setting):
                attrs.pop(key)
                setting.name = setting.name or key.lower()
                settings[setting.name] = as_test_setting(setting)
        if not attrs.pop('virtual', False):
            setting_name = attrs.pop('name', name).lower()
            if setting_name:
                def_flag = '--%s' % setting_name.replace(
                    ' ', '-').replace('_', '-')
                action = attrs.pop('action', None)
                type = attrs.pop('type', None)
                default = attrs.pop('default', None)
                validator = attrs.pop('validator', None)
                nargs = attrs.pop('nargs', None)
                if (validator is None and default is None and type is None and
                        nargs is None):
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
        return super().__new__(cls, name, bases, attrs)


class TestPlugin(Plugin, metaclass=TestPluginMeta):
    '''Base class for :class:`.Plugin` which can be added to a
    :class:`.TestSuite` to extend its functionalities.

    If the class attribute :attr:`name` is not specified or its value validate
    as ``True``, an additional :ref:`setting <settings>` is added to the
    configuration.
    In addition, a :class:`TestPlugin` can specify several additional
    :ref:`settings <settings>` as class attributes. For example, the
    :ref:`benchmark plugin <bench-plugin>` has an additional setting
    for controlling the number of repetitions::

        class Bench(TestPlugin):
            repeat = pulsar.Setting(type=int,
                                default=1,
                                validator=pulsar.validate_pos_int,
                                desc="Default number of repetition")


    .. attribute:: name

        Class attribute used for adding the default plugin
        :ref:`setting <settings>`
        to the configuration container of the test suite application.
        If the attribute is not set, the class name in lower case is used.
        If set and validate as not ``True``, no new :ref:`setting <settings>`
        is added to the test suite configuration parameters. For example::

            class MyPlugin(TestPlugin):
                name = None

        won't add the default plugin :ref:`setting <settings>`.

    .. attribute:: desc

        Class attribute used as the description of the
        :ref:`setting <settings>`.
        If :attr:`name` is disabled, this attribute is not relevant.

    .. attribute:: config

        A :class:`.Config` container created by the :class:`TestPlugin`
        metaclass. It collects the default setting, if
        available, and any additional :ref:`settings <settings>` specified
        as class attributes.
    '''
    virtual = True

    def __new__(cls):
        o = super().__new__(cls)
        o.config = cls.config.copy()
        return o

    def configure(self, cfg):
        self.config = cfg
