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
from pulsar.utils import system, colors
from pulsar.utils.config import Config
#from pulsar.utils import debug


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
    
    def __init__(self, usage=None):
        self.usage = usage
        self.cfg = None
        self.callable = None
        self.load_config()
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('logger',None)
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.configure_logging()
  
    def load_config(self):
        '''Load the application configuration'''
        self.cfg = Config(self.usage)
        
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
            except Exception as e:
                print("Failed to read config file: %s" % opts.config)
                traceback.print_exc()
                sys.exit(1)
        
            for k, v in list(cfg.items()):
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
        for k, v in list(opts.__dict__.items()):
            if v is None:
                continue
            self.cfg.set(k.lower(), v)
               
    def init(self, parser, opts, args):
        raise NotImplementedError
    
    def load(self):
        raise NotImplementedError

    def reload(self):
        self.load_config()
        if self.cfg.spew:
            debug.spew()
        loglevel = self.LOG_LEVELS.get(self.cfg.loglevel.lower(), logging.INFO)
        self.log.setLevel(loglevel)
        
    def handler(self):
        '''Returns a callable application handler, used by a :class:`pulsar.Worker`
to carry out its task.'''
        if self.callable is None:
            self.callable = self.load()
        return self.callable
    
    def run(self):
        if self.cfg.spew:
            debug.spew()
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
        self.log = pulsar.getLogger()

        handlers = []
        Formatter = logging.Formatter
        if self.cfg.logfile != "-":
            handlers.append(logging.FileHandler(self.cfg.logfile))
        else:
            Formatter = colors.ColorFormatter
            handlers.append(logging.StreamHandler())

        loglevel = self.LOG_LEVELS.get(self.cfg.loglevel.lower(), logging.INFO)
        self.log.setLevel(loglevel)
        
        format = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
        datefmt = r"%Y-%m-%d %H:%M:%S"
        for h in handlers:
            h.setFormatter(Formatter(format, datefmt))
            self.log.addHandler(h)


