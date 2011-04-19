import os
import sys

parentdir = lambda dir,up=1: dir if not up else parentdir(os.path.split(dir)[0],up-1)


def uplevel(path,lev):
    if lev:
        return uplevel(os.path.split(path)[0],lev-1)
    else:
        return path
    

def filedir(name):
    return parentdir(os.path.abspath(name))


class AddToPath(object):
    
    def __init__(self, local_dir):
        self.local_dir = local_dir
        
    def add(self, module = None, uplev = 0, down = None):
        '''Add a directory to the python path'''
        if module:
            try:
                __import__(module)
                return module
            except:
                pass
            
        dir = uplevel(self.local_dir,uplev)
        if down:
            dir = os.path.join(dir, *down)
        if os.path.isdir(dir) and dir not in sys.path:
            sys.path.insert(0,dir)
        if module:
            try:
                __import__(module)
                return module
            except:
                return None
