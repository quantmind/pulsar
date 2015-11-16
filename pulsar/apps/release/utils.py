

def change_version(manager, version):

    def _generate():
        with open(manager.cfg.version_file, 'r') as file:
            text = file.read()

        for line in text.split('\n'):
            if line.startswith('VERSION = '):
                yield 'VERSION = %s' % str(version)
            else:
                yield line

    text = '\n'.join(_generate())
    with open(manager.cfg.version_file, 'w') as file:
        file.write(text)
