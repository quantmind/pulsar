'''Sphinx extension for displaying Setting information
'''
from inspect import isfunction

from sphinx.util.compat import Directive
from docutils import nodes, statemachine

from pulsar.apps.ds import COMMANDS_INFO
from pulsar.utils.structures import OrderedDict

targetid = "redis_commands"


class redis_commands(nodes.General, nodes.Element):
    pass


class RedisCommands(Directive):
    has_content = False
    required_arguments = 0

    def sections(self):
        sec = {}
        unsupported = []
        sections = OrderedDict()
        for info in COMMANDS_INFO.values():
            if info.group not in sections:
                sections[info.group] = []
            group = sections[info.group]
            if info.supported:
                group.append(info)
            else:
                unsupported.append(info)
        return unsupported, sections

    def text(self):
        unsupported, sections = self.sections()
        if unsupported:
            yield '**Commands not yet supported**: %s' % self.links(unsupported)
            yield '\n'
        for section, commands in sections.items():
            s = section.lower().replace(' ', '-')
            yield ('.. _redis-{0}:\n\n''{1}\n'
                   '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n').format(s, section)
            yield self.links(commands)
            yield '\n'

    def links(self, commands):
        return ', '.join(('`%s <%s>`_' % (command.name, command.url) for
                          command in sorted(commands, key=lambda x: x.name)))

    def run(self):
        env = self.state.document.settings.env
        rawdocs = '\n'.join(self.text())

        source = self.state_machine.input_lines.source(
            self.lineno - self.state_machine.input_offset - 1)

        encoding = self.options.get(
            'encoding', self.state.document.settings.input_encoding)
        tab_width = self.options.get(
            'tab-width', self.state.document.settings.tab_width)


        if 'literal' in self.options:
            # Convert tabs to spaces, if `tab_width` is positive.
            if tab_width >= 0:
                text = rawtext.expandtabs(tab_width)
            else:
                text = rawtext
            literal_block = nodes.literal_block(rawtext, text, source=path)
            literal_block.line = 1
            return [literal_block]
        else:
            include_lines = statemachine.string2lines(
                rawdocs, tab_width, convert_whitespace=1)
            self.state_machine.insert_input(include_lines, targetid)
            return []


def setup(app):
    app.add_directive(targetid, RedisCommands)
