# -*- coding: utf-8 -*-
#

import os

import sphinx
if sphinx.version_info < (5, 2):
    import warnings
    warnings.warn("You should use sphinx 5.2+")

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.extlinks', 'sphinx.ext.intersphinx',
    "sphinx.ext.viewcode", "sphinx.ext.autosummary"]


extlinks={
    'issue': ('https://github.com/rogerbinns/apsw/issues/%s',
              'APSW issue %s'),
    'source': ('https://github.com/rogerbinns/apsw/blob/master/%s',
               '%s'),
    }

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General substitutions.
project = u'APSW'
copyright = u'2004-2023, Roger Binns <rogerb@rogerbinns.com>'
html_logo = "apswlogo.png"

# The default replacements for |version| and |release|, also used in various
# other places throughout the built documents.
#
# The short X.Y version.
version = os.getenv("VERSION")
# The full version, including alpha/beta/rc tags.
release = version
today = os.getenv("RELEASEDATE")
assert version and today

today_fmt = '%B %d, %Y'

exclude_trees = ['build']

add_module_names = False


# Options for HTML output
# -----------------------

html_theme = 'default'
html_theme_options = {
    'stickysidebar': True,
    'externalrefs': True
}
html_favicon = "favicon.ico"

html_static_path = ['.static']
html_last_updated_fmt = '%b %d, %Y'

htmlhelp_basename = 'apsw'


### Extra gunk

def skip_Shell_members(app, what, name, obj, skip, options):
    if name.startswith("command_") or name.startswith("output_"):
        return True
    return skip

def setup(app):
    app.connect('autodoc-skip-member', skip_Shell_members)

nitpicky = True