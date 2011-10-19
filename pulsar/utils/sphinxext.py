from pulsar.utils.config import KNOWN_SETTINGS

from sphinx.util.compat import Directive
from docutils import nodes, statemachine

targetid = "pulsar_settings"

class pulsar_settings(nodes.General, nodes.Element):
    pass


class PulsarSettings(Directive):
    has_content = False
    required_arguments = 0

    def text(self):
        for sett in KNOWN_SETTINGS:
            desc = sett.desc.strip()
            text = '.. _setting-{0}:\n\n\
{0}\n=====================================\n\n\
{1}\n'.format(sett.name,desc)
            yield text
            
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

    
