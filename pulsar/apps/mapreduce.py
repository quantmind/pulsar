from .base import Application


class MapReduce(Application):
    '''An application which maintains a pool of workers dedicated to solving
tasks.'''
    def init(self, parser, opts, args):
        if len(args) != 1:
            parser.error("No application module specified.")

        self.cfg.set("default_proc_name", args[0])
        self.app_uri = args[0]

        sys.path.insert(0, os.getcwd())
    

    
    