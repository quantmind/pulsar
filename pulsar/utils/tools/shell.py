
import subprocess


def local(command, capture=True):
    """
    Run a command on the local system.
    Function taken and adapted from fabric http://fabfile.org/

    ``local`` is simply a convenience wrapper around the use of the builtin
    Python ``subprocess`` module with ``shell=True`` activated. If you need to
    do anything special, consider using the ``subprocess`` module directly.

    `local` will, by default, capture and return the contents of the command's
    stdout as a string, and will not print anything to the user (the command's
    stderr is captured but discarded.)
    
    .. note::
        This differs from the default behavior of `run` and `sudo` due to the
        different mechanisms involved: it is difficult to simultaneously
        capture and print local commands, so we have to choose one or the
        other. We hope to address this in later releases.

    If you need full interactivity with the command being run (and are willing
    to accept the loss of captured stdout) you may specify ``capture=False`` so
    that the subprocess' stdout and stderr pipes are connected to your terminal
    instead of captured by Fabric.

    When ``capture`` is False, global output controls (``output.stdout`` and
    ``output.stderr`` will be used to determine what is printed and what is
    discarded.
    """
    # Handle cd() context manager
    cwd = ''
    # Construct real command
    real_command = cwd + command
    if output.debug:
        print("[localhost] run: %s" % (real_command))
    elif output.running:
        print("[localhost] run: " + command)
    # By default, capture both stdout and stderr
    PIPE = subprocess.PIPE
    out_stream = PIPE
    err_stream = PIPE
    # Tie in to global output controls as best we can; our capture argument
    # takes precedence over the output settings.
    if not capture:
        if output.stdout:
            out_stream = None
        if output.stderr:
            err_stream = None
    p = subprocess.Popen([real_command], shell=True, stdout=out_stream,
            stderr=err_stream)
    (stdout, stderr) = p.communicate()
    # Handle error condition (deal with stdout being None, too)
    out = _AttributeString(stdout.strip() if stdout else "")
    out.failed = False
    out.return_code = p.returncode
    if p.returncode != 0:
        out.failed = True
        msg = "local() encountered an error (return code %s) while executing '%s'" % (p.returncode, command)
        _handle_failure(message=msg)
    # If we were capturing, this will be a string; otherwise it will be None.
    return out
