
if __name__ == '__main__':
    from pulsar.apps.test import TestSuite
    test_suite = TestSuite(test_modules=('tests', 'examples'))
    test_suite.start()
