"""
Test init module functions
"""
from unittest import mock

import pytest

import featurebyte as fb
from featurebyte.config import Configurations


def _assert_tutorial_profile_with_api_token(api_token: str) -> None:
    """
    Assert that there's a tutorial profile with the given api token
    """
    # Reload configs and verify that there's now a profile with tutorial
    config = Configurations()
    has_tutorial_profile = False
    for profile in config.profiles:
        if profile.name == "tutorial":
            assert not has_tutorial_profile  # check that there's only one tutorial profile
            has_tutorial_profile = True
            assert profile.api_token == api_token
    assert has_tutorial_profile

    # Verify that the active profile is now the tutorial profile
    assert config.profile.name == "tutorial"


@pytest.fixture(name="noop_check_sdk_versions")
def patch_check_sdk_versions_fixture():
    """
    Patch check_sdk_versions function
    """
    with mock.patch("featurebyte.config.Configurations.check_sdk_versions"):
        yield


@pytest.fixture(name="noop_log_env_summary")
def patch_log_env_summary_fixture():
    """
    Patch log_env_summary function to be a no-op
    """
    with mock.patch("featurebyte.log_env_summary"):
        yield


def test_register_tutorial_api_token(noop_check_sdk_versions, noop_log_env_summary):
    """
    Test register_tutorial_api_token function
    """
    _ = noop_check_sdk_versions, noop_log_env_summary
    config = Configurations()
    tutorial_profile_name = "tutorial"
    # Verify that there's no profile with tutorial
    for profile in config.profiles:
        assert profile.name != tutorial_profile_name
    # Verify that the active profile is not tutorial
    original_profile_name = config.profile.name
    assert original_profile_name != "tutorial"

    try:
        # Register tutorial api token
        fb.register_tutorial_api_token("test")
        _assert_tutorial_profile_with_api_token("test")

        # Re-register with different API token
        fb.register_tutorial_api_token("test2")
        _assert_tutorial_profile_with_api_token("test2")
    finally:
        # Reset back to original profile
        config.use_profile(original_profile_name)
