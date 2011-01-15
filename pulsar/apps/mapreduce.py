

from pulsar.app.base import Application




class MapReduce(Application):
    
    def init(self, parser, opts, args):
        if len(args) != 1:
            parser.error("No application module specified.")

        self.cfg.set("default_proc_name", args[0])
        self.app_uri = args[0]

        sys.path.insert(0, os.getcwd())
    
    
def run():
    """\
    The ``gunicorn`` command line runner for launcing Gunicorn with
    generic WSGI applications.
    """
    from gunicorn.app.wsgiapp import WSGIApplication
    WSGIApplication("%prog [OPTIONS] APP_MODULE").run()
    
    