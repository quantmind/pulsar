import sys
import os
import platform

from runtests import run


if __name__ == '__main__':
    if platform.python_implementation() == 'PyPy' and '--pep8' in sys.argv:
        sys.exit(0)     # don't run pep8 on pypySSSSSSSS
    if sys.version_info > (3, 3):
        run(coverage=True, show_leaks=2, coveralls=True)
    else:
        run()
