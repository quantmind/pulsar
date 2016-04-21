import os
import tempfile

__all__ = ['Pidfile']


class Pidfile:
    """\
    Manage a PID file. If a specific name is provided
    it and '"%s.oldpid" % name' will be used. Otherwise
    we create a temp file using os.mkstemp.
    """
    def __init__(self, fname=None):
        self.fname = fname
        self.pid = None

    def create(self, pid=None):
        pid = pid or os.getpid()
        self.pid = self.read()
        if self.pid and self.pid != pid:
            if self.exists:
                raise RuntimeError("Already running on PID %s "
                                   "(pid file '%s')" %
                                   (self.pid, self.fname))
        self.pid = pid
        # Write pidfile
        if self.fname:
            fdir = os.path.dirname(self.fname)
            if fdir and not os.path.isdir(fdir):
                raise RuntimeError("%s doesn't exist. Can't create pidfile."
                                   % fdir)
        else:
            self.fname = tempfile.mktemp()
        with open(self.fname, 'w') as f:
            f.write("%s\n" % self.pid)
        # set permissions to -rw-r--r--
        os.chmod(self.fname, 420)

    def rename(self, path):
        self.unlink()
        self.fname = path
        self.create(self.pid)

    def unlink(self):
        """ delete pidfile"""
        try:
            with open(self.fname, "r") as f:
                pid1 = int(f.read() or 0)
            if pid1 == self.pid:
                os.unlink(self.fname)
        except Exception:
            pass

    def read(self):
        """ Validate pidfile and make it stale if needed"""
        if not self.fname:
            return
        try:
            with open(self.fname, "r") as f:
                wpid = int(f.read() or 0)
                if wpid <= 0:
                    return
                return wpid
        except IOError:
            return

    @property
    def exists(self):
        if self.pid:
            try:
                os.kill(self.pid, 0)
                return True
            except ProcessLookupError:
                pass
        return False
