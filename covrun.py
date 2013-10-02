import sys
import os

from runtests import run


if __name__ == '__main__':
    if sys.version_info > (3, 3):
        run(coverage=True, show_leaks=True, coveralls=True)
    else:
        run()
