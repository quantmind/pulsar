import os
import shutil


def rmgeneric(path, __func__):
    try:
        __func__(path)
        #print 'Removed ', path
        return 1
    except OSError as e:
        print('Could not remove {0}, {1}'.format(path, e))
        return 0


def rmfiles(path, ext=None, rmcache=True):
    if not os.path.isdir(path):
        return 0
    trem = 0
    tall = 0
    files = os.listdir(path)
    for f in files:
        fullpath = os.path.join(path, f)
        if os.path.isfile(fullpath):
            sf = f.split('.')
            if len(sf) == 2:
                if ext is None or sf[1] == ext:
                    tall += 1
                    trem += rmgeneric(fullpath, os.remove)
        elif f == '__pycache__' and rmcache:
            shutil.rmtree(fullpath)
            tall += 1
        elif os.path.isdir(fullpath):
            r, ra = rmfiles(fullpath, ext)
            trem += r
            tall += ra
    return trem, tall


if __name__ == '__main__':
    path = os.curdir
    removed, allfiles = rmfiles(path, 'pyc')
    print('removed {0} pyc files out of {1}'.format(removed, allfiles))
