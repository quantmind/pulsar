import os
import setup

script = os.path.abspath(setup.__file__)

assert setup.mod.VERSION[3] == 'final'

with open('CHANGELOG.rst', 'r') as f:
    changelog = f.read()

top = changelog.split('\n')[0]

assert top == 'Ver. %s' % setup.mod.__version__


setup.run(argv=[script, 'sdist'])


print('%s %s ready!' % (setup.package_name, setup.mod.__version__))
