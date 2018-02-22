"""Set of utility functions for OTU 16S"""

import os
import logging
import time

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
def now(dateformat="%Y-%m-%d %H:%M"):
    """return datetime stamp of NOW"""
    return time.strftime(dateformat)


# Log It!
def log_it(logname=os.path.basename(__file__), logdir="logs"):
    """package logging setup"""
    curtime = now("%Y%m%d-%H%M")
    logfile = '.'.join([curtime, logname, 'log'])
    logfile = os.path.join(logdir, logfile)

    loglevel = logging.DEBUG
    logFormat = "%(asctime)s %(levelname)5s: %(module)15s %(funcName)10s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat, filename=logfile, level=loglevel)
    l = logging.getLogger(logname)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(formatter)
    l.addHandler(ch)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    l.addHandler(fh)

    return l


# dump_args decorator
# orig from: https://wiki.python.org/moin/PythonDecoratorLibrary#Easy_Dump_of_Function_Arguments
def dump_args(func):
    """This decorator dumps out the arguments passed to a function before calling it.
    use example:
    @dump_args
    def f1(a,b,c):
        print(a + b + c)
    
    f1(1, 2, 3)
    """
    argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
    fname = func.func_name

    def func_args(*args,**kwargs):
        log.debug("'{}' args: {}".format(
            fname, ', '.join('%s=%r' % entry
                for entry in zip(argnames,args) + kwargs.items())) )
            # "'"+fname+"' args: "+', '.join(
            # '%s=%r' % entry
            # for entry in zip(argnames,args) + kwargs.items()))
        return func(*args, **kwargs)

    return func_args


