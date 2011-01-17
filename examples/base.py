import os
import sys

parent = lambda p : os.path.split(p)[0]

local_dir = parent(parent(os.path.abspath(__file__)))
if local_dir not in sys.path:
    sys.path.insert(0,local_dir)