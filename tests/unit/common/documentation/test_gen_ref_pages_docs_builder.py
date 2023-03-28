"""
Test gen ref pages docs builder.
"""
import pytest

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.gen_ref_pages_docs_builder import DocsBuilder


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
