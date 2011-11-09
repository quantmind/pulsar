import sys

if sys.version_info >= (2,7):
    import unittest as test
else:
    try:
        import unittest2 as test
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unitest2 package')
        exit(0)
