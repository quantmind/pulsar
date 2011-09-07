'''\
Benchmark pulsar using `pulsar.apps.tempo` application    
'''
from pulsar.apps.tempo import Application

if __name__ == '__main__':
    Application().start()