"""
Test doc types
"""
from featurebyte import version
from featurebyte.common.documentation.doc_types import get_docs_version


def test_get_docs_version():
    """
    Test get_docs_version
    """
    current_version = version
    split_current_version = current_version.split(".")
    # assert that the current version is in the format x.y.z
    assert len(split_current_version) == 3
    expected_version = ".".join(split_current_version[:2])
    assert get_docs_version() == expected_version
