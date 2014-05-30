import sys

try:
    import pep8
except ImportError:
    pep8 = None


def pep8_run(paths, config_file=None, stream=None):
    '''Programmatically run ``pep8``.

    Returns a 2-elements tuple with a string message and an exit code.
    '''
    if pep8:
        stream = stream or sys.stderr
        stream.write('Running pep8\n')
        pep8style = pep8.StyleGuide(paths=paths, config_file=config_file)
        options = pep8style.options
        report = pep8style.check_files()
        if options.statistics:
            report.print_statistics()
        if options.benchmark:
            report.print_benchmark()
        if options.testsuite and not options.quiet:
            report.print_results()
        if report.total_errors:
            msg = str(report.total_errors) + '\n' if options.count else ''
            return msg, 1
        return 'OK', 0
    return 'pep8 not installed', 1
