# -*- coding: utf-8 -*-
#

import os


# monkey patch https://github.com/sphinx-doc/sphinx/issues/11253
def split(self, input):
    res = []
    for word in sphinx.search.SearchLanguage.split(self, input):
        res.extend(word.split("_"))
    return res


import sphinx.search

sphinx.search.SearchEnglish.split = split
del split

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
]

if not os.getenv("APSW_NO_GA"):
    extensions.append("sphinxcontrib.googleanalytics")
    googleanalytics_id = "G-2NR9GDCQLT"


# how we link to various things
rst_prolog = """
.. |anyio| replace:: :external+anyio:doc:`anyio <index>`

.. |uvloop| replace:: `uvloop <https://uvloop.readthedocs.io/>`__

.. |trio| replace:: :external+trio:doc:`trio <index>`

.. |aiosqlite| replace:: `aiosqlite <https://aiosqlite.omnilib.dev/en/stable/>`__

.. |badge-async-sync| replace::  :ref:`Sync only <badge_async_sync>`

.. |badge-async-async| replace::  :ref:`Async only <badge_async_async>`

.. |badge-async-dual| replace::  :ref:`Sync / Async <badge_async_dual>`

.. |badge-async-value| replace::  :ref:`Value <badge_async_value>`

.. |badge-close| replace:: :ref:`Close <badge_close>`

"""


# this shows shorter names like Buffer instead of collections.abc.Buffer
python_use_unqualified_type_names = True
# less verbose Literal [ "one", "two"] -> "one" : "two"
python_display_short_literal_types = True

autodoc_preserve_defaults = True

extlinks = {
    "issue": ("https://github.com/rogerbinns/apsw/issues/%s", "APSW issue %s"),
    "source": ("https://github.com/rogerbinns/apsw/blob/master/%s", "%s"),
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "trio": (" https://trio.readthedocs.io/en/stable/", None),
    "anyio": (" https://anyio.readthedocs.io/en/stable/", None),
    }

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

pygments_style = "vs"

# General substitutions.
project = "APSW"
author = "Roger Binns <rogerb@rogerbinns.com>"
copyright = f"2004-2026, { author }"
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

today_fmt = "%B %d, %Y"

exclude_trees = ["build"]
exclude_patterns = ["fts.rst", "cli.rst"]

extlinks_detect_hardcoded_links = True

# currently broken - see https://github.com/readthedocs/sphinx_rtd_theme/issues/1529
# maximum_signature_line_length = 40

# Options for HTML output

html_title = f"{ project } { version } documentation"
html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "prev_next_buttons_location": "both",
}
html_css_files = ["apsw.css"]
html_baseurl = "https://rogerbinns.github.io/apsw/"

html_favicon = "favicon.ico"

html_static_path = ["_static"]
html_last_updated_fmt = "%b %d, %Y"

# One page html
singlehtml_sidebars = {"index": ["globaltoc.html"]}

# epub

epub_basename = f"{ project } { version }"
epub_description = f"Documentation for APSW { version } Python package"
epub_identifier = "https://github.com/rogerbinns/apsw"
epub_scheme = "URL"
epub_cover = (html_logo, "")
viewcode_enable_epub = True
epub_theme = "default"
epub_theme_options = {"nosidebar": True, "externalrefs": True, "globaltoc_maxdepth": 1}
epub_show_urls = "no"

# pdf using rst2pdf
pdf_documents = [
    ("index", f"{ project } { version }", html_title, author),
]

pdf_stylesheets = ["sphinx", "a4"]
pdf_fit_mode = "shrink"

# latexpdf
latex_engine = "lualatex"
latex_logo = html_logo

### Extra gunk


def skip_Shell_members(app, what, name, obj, skip, options):
    if (name.startswith("command_") or name.startswith("output_")) and "Shell." in str(obj):
        return True
    return skip


def setup(app):
    app.connect("autodoc-skip-member", skip_Shell_members)


nitpicky = True

nitpick_ignore = [
    ('py:class', 'apsw.aio.T'),
    # private
    ('py:class', 'apsw.aio._CallTracker'),
    # gets confused by C extension
    ('py:class', '_queue.SimpleQueue'),
]

# autosummary etc fail to import modules even though python import
# works just fine, so we cheat by importing them here

import apsw, apsw.aio
