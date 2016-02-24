# Autoreloading launcher.
# Borrowed from Peter Hunt and the CherryPy project (http://www.cherrypy.org).
# Some taken from Ian Bicking's Paste (http://pythonpaste.org/).
#
# Portions copyright (c) 2004, CherryPy Team (team@cherrypy.org)
# All rights reserved.

import os
import signal
import sys

try:
    import termios
except ImportError:
    termios = None

USE_INOTIFY = False
try:
    # Test whether inotify is enabled and likely to work
    import pyinotify

    fd = pyinotify.INotifyWrapper.create().inotify_init()
    if fd >= 0:
        USE_INOTIFY = True
        os.close(fd)
except ImportError:     # pragma    nocover
    pass

EXIT_CODE = 5

FILE_MODIFIED = 1
I18N_MODIFIED = 2

_mtimes = {}
_win = (sys.platform == "win32")

_error_files = []
_cached_modules = set()
_cached_filenames = []


def gen_filenames(only_new=False):
    """Returns a list of filenames referenced in sys.modules and translation
    files.
    """
    global _cached_modules, _cached_filenames
    module_values = set(sys.modules.values())
    if _cached_modules == module_values:
        # No changes in module list, short-circuit the function
        if only_new:
            return []
        else:
            return _cached_filenames

    new_modules = module_values - _cached_modules
    new_filenames = [filename.__file__ for filename in new_modules
                     if hasattr(filename, '__file__')]

    if only_new:
        filelist = new_filenames
    else:
        filelist = _cached_filenames + new_filenames + _error_files
    filenames = []
    for filename in filelist:
        if not filename:
            continue
        if filename.endswith(".pyc") or filename.endswith(".pyo"):
            filename = filename[:-1]
        if filename.endswith("$py.class"):
            filename = filename[:-9] + ".py"
        if os.path.exists(filename):
            filenames.append(filename)
    _cached_modules = _cached_modules.union(new_modules)
    _cached_filenames += new_filenames
    return filenames


def inotify_code_changed():
    """
    Checks for changed code using inotify. After being called
    it blocks until a change event has been fired.
    """
    class EventHandler(pyinotify.ProcessEvent):
        modified_code = None

        def process_default(self, event):
            if event.path.endswith('.mo'):
                EventHandler.modified_code = I18N_MODIFIED
            else:
                EventHandler.modified_code = FILE_MODIFIED

    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm, EventHandler())

    def update_watch(sender=None, **kwargs):
        if sender and getattr(sender, 'handles_files', False):
            return
        mask = (
            pyinotify.IN_MODIFY |
            pyinotify.IN_DELETE |
            pyinotify.IN_ATTRIB |
            pyinotify.IN_MOVED_FROM |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE
        )
        for path in gen_filenames(only_new=True):
            wm.add_watch(path, mask)

    # Block until an event happens.
    update_watch()
    notifier.check_events(timeout=None)
    notifier.read_events()
    notifier.process_events()
    notifier.stop()

    # If we are here the code must have changed.
    return EventHandler.modified_code


def code_changed():
    global _mtimes, _win
    for filename in gen_filenames():
        stat = os.stat(filename)
        mtime = stat.st_mtime
        if _win:
            mtime -= stat.st_ctime
        if filename not in _mtimes:
            _mtimes[filename] = mtime
            continue
        if mtime != _mtimes[filename]:
            _mtimes = {}
            try:
                del _error_files[_error_files.index(filename)]
            except ValueError:
                pass
            return True
    return False


def ensure_echo_on():
    if termios:
        fd = sys.stdin
        if fd.isatty():
            attr_list = termios.tcgetattr(fd)
            if not attr_list[3] & termios.ECHO:
                attr_list[3] |= termios.ECHO
                if hasattr(signal, 'SIGTTOU'):
                    old_handler = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
                else:
                    old_handler = None
                termios.tcsetattr(fd, termios.TCSANOW, attr_list)
                if old_handler is not None:
                    signal.signal(signal.SIGTTOU, old_handler)


def check_changes():
    ensure_echo_on()
    if USE_INOTIFY:
        return inotify_code_changed()
    else:
        return code_changed()


def restart_with_reloader():
    while True:
        args = [sys.executable] + ['-W%s' % o for o in
                                   sys.warnoptions] + sys.argv
        if _win:
            args = ['"%s"' % arg for arg in args]
        new_environ = os.environ.copy()
        new_environ["RUN_MAIN"] = 'true'
        exit_code = os.spawnve(os.P_WAIT, sys.executable, args, new_environ)
        if exit_code != EXIT_CODE:
            return exit_code


def start():
    if os.environ.get("RUN_MAIN") != "true":
        try:
            exit_code = restart_with_reloader()
            if exit_code < 0:
                os.kill(os.getpid(), -exit_code)
            else:
                sys.exit(exit_code)
        except KeyboardInterrupt:
            pass
        return True
