from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES
import os
import sys

PACKAGE_NAME  = 'pulsar'

root_dir      = os.path.split(os.path.abspath(__file__))[0]
package_dir   = os.path.join(root_dir, PACKAGE_NAME) 

def get_module():
    if root_dir not in sys.path:
        sys.path.insert(0,root_dir)
    return __import__(PACKAGE_NAME)

mod = get_module()


def read(fname):
    return open(os.path.join(root_dir, fname)).read()

def requirements():
    req = read('requirements.txt').replace('\r','').split('\n')
    result = []
    for r in req:
        r = r.replace(' ','')
        if r:
            result.append(r)
    return result    

class osx_install_data(install_data):

    def finalize_options(self):
        self.set_undefined_options('install', ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

if sys.platform == "darwin": 
    cmdclasses = {'install_data': osx_install_data} 
else: 
    cmdclasses = {'install_data': install_data} 
    
# Tell distutils to put the data_files in platform-specific installation
# locations. See here for an explanation:
# http://groups.google.com/group/comp.lang.python/browse_thread/thread/35ec7b2fed36eaec/2105ee4d9e8042cb
for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

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

for dirpath, dirnames, filenames in os.walk(package_dir):
    # Ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith('.'): del dirnames[i]
    if '__init__.py' in filenames:
        packages.append('.'.join(fullsplit(dirpath)[len_root_dir:]))
    elif filenames:
        rel_dir = get_rel_dir(dirpath,root_dir)
        data_files.append([rel_dir, [os.path.join(dirpath, f) for f in filenames]])

if len(sys.argv) > 1 and sys.argv[1] == 'bdist_wininst':
    for file_info in data_files:
        file_info[0] = '\\PURELIB\\%s' % file_info[0]


setup(
        name         = PACKAGE_NAME,
        version      = mod.get_version(),
        author       = mod.__author__,
        author_email = mod.__contact__,
        url          = mod.__homepage__,
        license      = mod.__license__,
        description  = mod.__doc__,
        long_description = read('README.rst'),
        packages     = packages,
        cmdclass     = cmdclasses,
        data_files   = data_files,
        install_requires = requirements(),
        classifiers = [
            'Development Status :: 1 - Planning',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Framework :: Django',
            'Programming Language :: JavaScript',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Topic :: Utilities'
        ],
    )

