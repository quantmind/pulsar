# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import errno
import logging
import os
import sys
import traceback

import pulsar
from pulsar.utils.py2py3 import execfile
from pulsar.utils import system, colors
from pulsar.utils.importer import import_module 
#from pulsar.utils import debug

__all__ = ['Application',
           'require']


def require(appname):
    apps = appname.split('.')
    if len(apps) == 1:
        module = 'pulsar.apps.{0}'.format(appname)
    else:
        module = appname
    mod = import_module(module)
    return mod


class Application(object):
    """\
    An application interface for configuring and loading
    the various necessities for any given web framework.
    """
    Arbiter = pulsar.Arbiter
    
    LOG_LEVELS = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG
    }
    
    def __init__(self, usage=None, callable = None, **params):
        self.usage = usage
        self.cfg = None
        self.callable = callable
        self.load_config(**params)
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('log',None)
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.configure_logging()
  
    def load_config(self, **params):
        '''Load the application configuration'''
        self.cfg = pulsar.Config(self.usage)
        
        # add params
        for k, v in params.items():
            self.cfg.set(k.lower(), v)
                
        # parse console args
        parser = self.cfg.parser()
        opts, args = parser.parse_args()
        
        # optional settings from apps
        cfg = self.init(parser, opts, args)
        
        # Load up the any app specific configuration
        if cfg:
            for k, v in list(cfg.items()):
                self.cfg.set(k.lower(), v)
                
        # Load up the config file if its found.
        if opts.config and os.path.exists(opts.config):
            cfg = {
                "__builtins__": __builtins__,
                "__name__": "__config__",
                "__file__": opts.config,
                "__doc__": None,
                "__package__": None
            }
            try:
                execfile(opts.config, cfg, cfg)
            except Exception:
                print("Failed to read config file: %s" % opts.config)
                traceback.print_exc()
                sys.exit(1)
        
            for k, v in cfg.items():
                # Ignore unknown names
                if k not in self.cfg.settings:
                    continue
                try:
                    self.cfg.set(k.lower(), v)
                except:
                    sys.stderr.write("Invalid value for %s: %s\n\n" % (k, v))
                    raise
            
        # Lastly, update the configuration with any command line
        # settings.
        for k, v in opts.__dict__.items():
            if v is None:
                continue
            self.cfg.set(k.lower(), v)
               
    def init(self, parser, opts, args):
        raise NotImplementedError
    
    def load(self):
        raise NotImplementedError

    def reload(self):
        self.load_config()
        loglevel = self.LOG_LEVELS.get(self.cfg.loglevel.lower(), logging.INFO)
        self.log.setLevel(loglevel)
        
    def handler(self):
        '''Returns a callable application handler, used by a :class:`pulsar.Worker`
to carry out its task.'''
        if self.callable is None:
            self.callable = self.load()
        return self.callable
    
    def run(self):
        if self.cfg.daemon:
            system.daemonize()
        else:
            try:
                system.setpgrp()
            except OSError as e:
                if e[0] != errno.EPERM:
                    raise
                    
        self.configure_logging()
        try:
            self.Arbiter(self).start()
        except RuntimeError as e:
            sys.stderr.write("\nError: %s\n\n" % e)
            sys.stderr.flush()
            sys.exit(1)
    
    def configure_logging(self):
        """\
        Set the log level and choose the destination for log output.
        """
        log = pulsar.getLogger()

        handlers = []
        Formatter = logging.Formatter
        if self.cfg.logfile != "-":
            handlers.append(logging.FileHandler(self.cfg.logfile))
        else:
            Formatter = colors.ColorFormatter
            handlers.append(logging.StreamHandler())

        if self.cfg.debug:
            loglevel = logging.DEBUG
        else:
            loglevel = self.LOG_LEVELS.get(self.cfg.loglevel.lower(), logging.INFO)
        log.setLevel(loglevel)
        
        format = '%(asctime)s [p=%(process)s,t=%(thread)s] [%(levelname)s] [%(name)s] %(message)s'
        #format = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
        datefmt = r"%Y-%m-%d %H:%M:%S"
        for h in handlers:
            h.setFormatter(Formatter(format, datefmt))
            log.addHandler(h)


