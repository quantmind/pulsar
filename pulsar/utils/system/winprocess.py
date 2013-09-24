import sys
import os
from multiprocessing import forking, process, freeze_support
from multiprocessing.util import _logger, _log_to_stderr

WINEXE = forking.WINEXE


def get_preparation_data(name):
    '''
    Return info about parent needed by child to unpickle process object.
    Monkey-patch from
    '''
    d = dict(
        name=name,
        sys_path=sys.path,
        sys_argv=sys.argv,
        log_to_stderr=_log_to_stderr,
        orig_dir=process.ORIGINAL_DIR,
        authkey=process.current_process().authkey,
    )

    if _logger is not None:
        d['log_level'] = _logger.getEffectiveLevel()

    if not WINEXE:
        main_path = getattr(sys.modules['__main__'], '__file__', None)
        if not main_path and sys.argv[0] not in ('', '-c'):
            main_path = sys.argv[0]
        if main_path is not None:
            if (not os.path.isabs(main_path) and process.ORIGINAL_DIR
                    is not None):
                main_path = os.path.join(process.ORIGINAL_DIR, main_path)
            if not main_path.endswith('.exe'):
                d['main_path'] = os.path.normpath(main_path)

    return d


forking.get_preparation_data = get_preparation_data
freeze_support()
