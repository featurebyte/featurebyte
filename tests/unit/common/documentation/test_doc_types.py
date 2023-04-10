"""
Test doc types
"""
import pytest

from featurebyte import version
from featurebyte.common.documentation.doc_types import (
    get_docs_version,
    update_description_with_version,
)


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


DOC_VERSION = get_docs_version()


@pytest.mark.parametrize(
    "input_description, expected_description",
    [
        ("random string", "random string"),
        ("/reference/random string", f"/{DOC_VERSION}/reference/random string"),
        (f"/{DOC_VERSION}/reference/random string", f"/{DOC_VERSION}/reference/random string"),
        (
            f"/reference/{DOC_VERSION}_in_string",
            f"/{DOC_VERSION}/reference/{DOC_VERSION}_in_string",
        ),
    ],
)
def test_update_description_with_version(input_description, expected_description):
    actual_description = update_description_with_version(input_description)
    assert actual_description == expected_description
