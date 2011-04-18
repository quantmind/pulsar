try:
    import pulsar
except ImportError:
    import os
    import sys
    p = lambda x : os.path.split(x)[0]
    path = p(p(p(os.path.abspath(__file__))))
    sys.path.insert(0,path)
    import pulsar
