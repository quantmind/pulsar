import os
import shutil

remove_dirs = ('dist', 'build', 'pulsar.egg-info')


def rmgeneric(path, __func__):
    try:
        __func__(path)
        return 1
    except OSError as e:
        print('Could not remove {0}, {1}'.format(path, e))
        return 0


def rmfiles(path=None, *extensions):
    path = path or os.curdir
    if not os.path.isdir(path):
        return 0
    assert extensions
    for ext in extensions:
        assert ext
    trem = 0
    tall = 0
    files = os.listdir(path)
    for name in files:
        fullpath = os.path.join(path, name)
        if os.path.isfile(fullpath):
            sf = name.split('.')
            if len(sf) == 2 and sf[1] in extensions:
                tall += 1
                trem += rmgeneric(fullpath, os.remove)
        elif name == '__pycache__':
            shutil.rmtree(fullpath)
            tall += 1
        elif os.path.isdir(fullpath) and not name.startswith('.'):
            r, ra = rmfiles(fullpath, *extensions)
            trem += r
            tall += ra
    return trem, tall


def run():
    for path in remove_dirs:
        if os.path.isdir(path):
            print('Removing %s' % path)
            shutil.rmtree(path)
    removed, allfiles = rmfiles(os.curdir, 'pyc', 'DS_Store')
    print('removed {0} pyc files out of {1}'.format(removed, allfiles))


if __name__ == '__main__':
    run()
