# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

from os import path
import sys
import os


home_path = path.dirname(path.dirname(path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(home_path))
sys.path.append(os.path.abspath(home_path+'/src/meta'))

from src.meta import pypackage_meta

project = 'ESPA'
copyright = '2023, AE_GROUP'
author = 'AE_GROUP'
release = pypackage_meta.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc']
source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'restructuredtext'
}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
