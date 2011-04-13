# Add pulsr to python path if not available dusing development
try:
    import pulsar
except ImportError:
    import os
    import sys
    p = lambda x : os.path.split(x)[0]
    path = p(p(p(os.path.abspath(__file__))))
    if path not in sys.path:
        sys.path.insert(0,path)
        
from pulsar.apps import tasks

if __name__ == '__main__':
    '''To run this example simply type::
    
        python manage.py
    '''
    tasks.run()