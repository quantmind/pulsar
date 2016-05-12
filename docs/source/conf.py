# -*- coding: utf-8 -*-
#
import sys
import os
os.environ['BUILDING-PULSAR-DOCS'] = 'yes'
p = lambda x : os.path.split(x)[0]
source_dir = p(os.path.abspath(__file__))
ext_dir = os.path.join(source_dir,'_ext')
docs_dir = p(source_dir)
base_dir = p(docs_dir)
sys.path.insert(0, base_dir)
sys.path.insert(0, ext_dir)
import pulsar
from setup import meta

##################
from recommonmark.parser import CommonMarkParser
from datetime import date

source_suffix = ['.rst', '.md']
source_parsers = {
    '.md': CommonMarkParser,
}
year = date.today().year
version = release = pulsar.__version__

html_theme_options = {
    'logo': 'pulsar-logo.png',
    'github_user': 'quantmind',
    'github_repo': 'pulsar',
    'description': 'Concurrent framework for python 3.5 and above.'
}
html_sidebars = {
    '**': [
        'about.html', 'navigation.html', 'searchbox.html'
    ]
}

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#sys.path.insert(0, os.path.abspath('.'))

# -- General configuration -----------------------------------------------------

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.coverage',
              'sphinx.ext.extlinks',
              'sphinx.ext.intersphinx',
              'sphinx.ext.viewcode',
              #'pulsar.utils.docs.html',
              'alabaster',
              'pulsarext',
              'redisext']

# Beta version is published in github pages
if pulsar.VERSION[3] == 'beta':
    extensions.append('sphinxtogithub')
    analytics_id = 'UA-3900561-8'
else:
    analytics_id = 'UA-3900561-7'

html_theme_options['analytics_id'] = analytics_id
html_context = {'release_version': pulsar.VERSION[3] == 'final'}

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'pulsar'
copyright = '2011-%s, %s' % (year, meta['author'])

html_theme = 'alabaster'
pygments_style = 'sphinx'
templates_path = ['_templates']
html_static_path = ['_static']
html_theme_path = ['_theme']
exclude_trees = ['_build']

exclude_trees = []
html_additional_pages = {
}

# Add any paths that contain custom themes here, relative to this directory.
#html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
#html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#html_logo = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = 'favicon.ico'

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
#html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# If false, no module index is generated.
#html_domain_indices = True

# If false, no index is generated.
#html_use_index = True

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
#html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Output file base name for HTML help builder.
htmlhelp_basename = 'pulsardoc'


# -- Options for LaTeX output --------------------------------------------------

# The paper size ('letter' or 'a4').
#latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
#latex_font_size = '10pt'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
  ('index', 'pulsar.tex', 'Pulsar Documentation',
   'Luca Sbardella', 'manual'),
]

intersphinx_mapping = {
    'python': ('http://python.readthedocs.org/en/latest/', None),
}

extlinks = {'django': ('https://www.djangoproject.com/', None),
            'postgresql': ('http://www.postgresql.org/', None),
            'sqlalchemy': ('http://www.sqlalchemy.org/', None),
            'greenlet': ('http://greenlet.readthedocs.org/', None),
            'psycopg2coroutine': ('http://pythonhosted.org/psycopg2/advanced.html#support-for-coroutine-libraries', None)}
