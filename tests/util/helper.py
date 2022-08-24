"""
This module contains utility functions used in tests
"""
import sys
from contextlib import contextmanager
from unittest.mock import Mock


def assert_equal_with_expected_fixture(actual, fixture_filename, update_fixture=False):
    """Utility to check that actual is the same as the pre-generated fixture

    To update all fixtures automatically, pass --update-fixtures option when invoking pytest.
    """
    if update_fixture:
        with open(fixture_filename, "w", encoding="utf-8") as f_handle:
            f_handle.write(actual)
            f_handle.write("\n")
            raise AssertionError(
                f"Fixture {fixture_filename} updated, please set update_fixture to False"
            )

    with open(fixture_filename, encoding="utf-8") as f_handle:
        expected = f_handle.read()

    assert actual.strip() == expected.strip()


@contextmanager
def patch_import_package(package_path):
    """
    Mock import statement
    """
    mock_package = Mock()
    original_module = sys.modules.get(package_path)
    sys.modules[package_path] = mock_package
    try:
        yield mock_package
    finally:
        if original_module:
            sys.modules[package_path] = original_module
        else:
            sys.modules.pop(package_path)
