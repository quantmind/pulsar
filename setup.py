import os
import sys
from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES

from lib.setup import libparams, BuildFailed

package_name  = 'pulsar'
package_fullname = package_name
root_dir      = os.path.split(os.path.abspath(__file__))[0]
package_dir   = os.path.join(root_dir, package_name)

class osx_install_data(install_data):

    def finalize_options(self):
        self.set_undefined_options('install', ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

def get_module():
    if root_dir not in sys.path:
        sys.path.insert(0,root_dir)
    return __import__(package_name)

mod = get_module()

def read(fname):
    return open(os.path.join(root_dir, fname)).read()

def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join) in a
    platform-neutral way.
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)

# Compile the list of packages available, because distutils doesn't have
# an easy way to do this.
def get_rel_dir(d,base,res=''):
    if d == base:
        return res
    br,r = os.path.split(d)
    if res:
        r = os.path.join(r,res)
    return get_rel_dir(br,base,r)


packages, data_files = [], []
pieces = fullsplit(root_dir)
if pieces[-1] == '':
    len_root_dir = len(pieces) - 1
else:
    len_root_dir = len(pieces)

for dirpath, _, filenames in os.walk(package_dir):
    if '__init__.py' in filenames:
        packages.append('.'.join(fullsplit(dirpath)[len_root_dir:]))
    elif filenames and not dirpath.endswith('__pycache__'):
        rel_dir = get_rel_dir(dirpath, package_dir)
        data_files.extend((os.path.join(rel_dir, f) for f in filenames))

if len(sys.argv) > 1 and sys.argv[1] == 'bdist_wininst':
    for file_info in data_files:
        file_info[0] = '\\PURELIB\\%s' % file_info[0]


def run_setup(with_cext):
    if with_cext:
        params = libparams
    else:
        params = {'cmdclass': {}}
    if sys.platform == "darwin":
        params['cmdclass']['install_data'] = osx_install_data
    else:
        params['cmdclass']['install_data'] = install_data
    
    params.update({'name': package_fullname,
                   'version': mod.__version__,
                   'author': mod.__author__,
                   'author_email': mod.__contact__,
                   'url': mod.__homepage__,
                   'license': mod.__license__,
                   'description': mod.__doc__,
                   'long_description': read('README.rst'),
                   'packages': packages,
                   'package_data': {package_name: data_files},
                   'classifiers':  mod.CLASSIFIERS})
    setup(**params)
    
def status_msgs(*msgs):
    print('*' * 75)
    for msg in msgs:
        print(msg)
    print('*' * 75)
    
try:
    run_setup(True)
except BuildFailed as exc:
    status_msgs(
            exc.msg,
            "WARNING: The C extension could not be compiled, " +
                "speedups are not enabled.",
            "Failure information, if any, is above.",
            "Retrying the build without the C extension now."
        )

    run_setup(False)

    status_msgs(
        "WARNING: The C extension could not be compiled, " +
            "speedups are not enabled.",
        "Plain-Python build succeeded."
    )

