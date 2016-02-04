"""Sphinx extension for displaying Setting information
"""
from inspect import isfunction

from sphinx.util.compat import Directive
from docutils import nodes, statemachine

from pulsar.utils.config import ordered_settings, section_docs
import pulsar.apps.wsgi
import pulsar.apps.test

targetid = "pulsar_settings"


class pulsar_settings(nodes.General, nodes.Element):
    pass


class PulsarSettings(Directive):
    has_content = False
    required_arguments = 0

    def sections(self):
        sec = {}
        sections = []
        for sett in ordered_settings():
            s = sett.section
            if s not in sec:
                sections.append(s)
                sec[s] = [sett]
            else:
                sec[s].append(sett)
        for s in sections:
            yield s, sec[s]

    def text(self):
        for section, settings in self.sections():
            if section is None:
                continue
            s = section.lower().replace(' ', '-')
            yield '.. _setting-section-{0}:\n\n\
{1}\n=====================================\n\n'.format(s, section)
            section_doc = section_docs.get(section)
            if section_doc:
                yield section_doc
                yield '\n'
            for sett in settings:
                desc = sett.desc.strip()
                yield '.. _setting-{0}:\n\n\
{0}\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n'.format(sett.name)
                if sett.app:
                    yield '*Application namespace*: ``%s``\n' % sett.app
                yield '*Config name*: ``%s``\n' % sett.name
                if sett.flags:
                    yield '*Command line*: %s\n' %\
                                ', '.join('``%s``' % f for f in sett.flags)
                if isfunction(sett.default):
                    default = ':func:`%s`' % sett.default.__name__
                else:
                    default = '``%r``' % sett.default
                yield '*Default*: %s\n' % default
                yield desc + '\n'

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
    app.add_directive(targetid, PulsarSettings)
