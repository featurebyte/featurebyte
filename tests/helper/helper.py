def assert_equal_with_expected_fixture(actual, fixture_filename, update_fixture=False):
    """Utility to check that actual is the same as the pre-generated fixture"""
    if update_fixture:
        with open(fixture_filename, "w", encoding="utf-8") as f_handle:
            f_handle.write(actual)
            raise AssertionError(
                f"Fixture {fixture_filename} updated, please set update_fixture to False"
            )

    with open(fixture_filename, encoding="utf-8") as f_handle:
        expected = f_handle.read()

    assert actual.strip() == expected.strip()
