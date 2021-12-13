# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("."))


# -- Project information -----------------------------------------------------

project = "OpenSarToolkit"
copyright = "2021, Andreas Vollrath"
author = "Andreas Vollrath"

# The full version, including alpha/beta/rc tags
release = "0.12.5"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autosummary", "sphinx.ext.viewcode", "nbsphinx"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["**.ipynb_checkpoints"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/ESA-PhiLab/OpenSarToolkit",
            "icon": "fab fa-github-square",
        }
    ],
    "github_url": "https://github.com/ESA-PhiLab/OpenSarToolkit",
    "show_nav_level": 2,
    "show_prev_next": False,
    "use_edit_page_button": True,
    "navigation_with_keys": False,
}
html_context = {
    "github_user": "ESA-PhiLab",
    "github_repo": "OpenSarToolkit",
    "github_version": "main",
    "doc_path": "docs/source",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# -- Options for autosummary output -------------------------------------------------
autosummary_generate = False

# -- Options for nb-sphinx output -------------------------------------------------
nbsphinx_execute = "never"
