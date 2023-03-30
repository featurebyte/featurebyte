"""
Test gen ref pages docs builder.
"""
from typing import Any, Set

import re
from contextlib import contextmanager
from dataclasses import dataclass

import pytest

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.gen_ref_pages_docs_builder import (
    MISSING_DEBUG_MARKDOWN,
    DocsBuilder,
)


class TestClassWithAutodoc:
    """
    Test class with FBAutodoc provided
    """

    __fbautodoc__ = FBAutoDoc(
        section=["Test"],
    )


class TestClassWithoutAutdoc:
    pass


@pytest.mark.parametrize(
    "test_class,expected_output,gen_docs_override",
    [
        (TestClassWithAutodoc, ["Test"], False),
        (TestClassWithoutAutdoc, None, False),
        (TestClassWithAutodoc, ["Test"], True),
        (
            TestClassWithoutAutdoc,
            ["test_gen_ref_pages_docs_builder", "TestClassWithoutAutdoc"],
            True,
        ),
    ],
)
def test_get_section_from_class_obj(test_class, expected_output, gen_docs_override):
    """
    Test get_section_from_class_obj
    """
    docs_builder = DocsBuilder(None, None, gen_docs_override)
    section = docs_builder.get_section_from_class_obj(test_class)
    assert section == expected_output


class NoopLinesWriter:
    """
    Noop lines writer class.
    """

    def writelines(self, lines):
        pass


class GenFilesOpenWrapper:
    """
    Simple wrapper class to help track all files that are opened.
    """

    def __init__(self):
        self.all_file_names = set()

    @contextmanager
    def gen_files_open(self, filename: str, mode: str = "w"):
        """
        Context manager that opens a file in memory.
        """
        _ = filename, mode
        self.all_file_names.add(str(filename))
        writer = NoopLinesWriter()
        yield writer


@pytest.fixture(name="noop_set_edit_path")
def noop_set_edit_path_fixture():
    """
    Noop set edit path fixture.
    """

    def _noop_set_edit_path(param1: Any, param2: Any):
        pass

    return _noop_set_edit_path


@dataclass
class NavItem:
    """
    Data class to represent a nav item.
    """

    property: str
    markdown_filename: str

    def is_missing(self, all_markdown_files: Set[str]):
        reference_file = f"reference/{self.markdown_filename}"
        return (
            MISSING_DEBUG_MARKDOWN in self.markdown_filename
            or reference_file not in all_markdown_files
        )


def _extract_nav_item(nav_item):
    if nav_item is None:
        return None
    # example line to extract info from: [EventTable.initialize_default_feature_job_settings](missing.md)
    regex = r"\[(.*)\]\((.*)\)"
    result = re.search(regex, nav_item)
    if result is None:
        return None
    return NavItem(result.group(1), result.group(2))


def test_all_docs(noop_set_edit_path):
    """
    Test that all docs are generated.

    There are 2 ways in which docs could go missing:
    - we are unable to infer the object path from the API path in the doc layout. This will result in the markdown file
      being `missing.md`
    - the doc_path_override is not generated. This means that the markdown file is not generated and the explicit
      override will be empty.
    These errors are likely to occur if we refactored the code, without updating the doc layout.
    """
    files_open_wrapper = GenFilesOpenWrapper()
    docs_builder = DocsBuilder(files_open_wrapper.gen_files_open, noop_set_edit_path)
    nav = docs_builder.build_docs()
    failed_items = []
    for nav_item in nav.build_literate_nav():
        extracted_nav_item = _extract_nav_item(nav_item)
        if extracted_nav_item is None:
            continue
        if extracted_nav_item.is_missing(files_open_wrapper.all_file_names):
            failed_items.append(extracted_nav_item)
    assert len(failed_items) == 0, f"Missing docs: {failed_items}"
