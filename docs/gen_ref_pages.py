"""
Generate the code reference pages and navigation.
"""
from mkdocs_gen_files import open as gen_files_open
from mkdocs_gen_files import set_edit_path

from featurebyte.common.documentation.gen_ref_pages_docs_builder import build_docs

build_docs(set_edit_path, gen_files_open)
