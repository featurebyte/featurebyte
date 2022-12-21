"""
Test app container
"""
from unittest.mock import patch

import pytest

from featurebyte.app import User
from featurebyte.routes.app_container import AppContainer, app_container_config
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import LocalTempStorage


def test_validate_configs():
    """
    Test validate configs
    """
    dict_with_duplicate_names_in_same_key = {
        "key1": [
            {
                "name": "name1",
            },
            {
                "name": "name1",
            },
        ]
    }
    with pytest.raises(ValueError):
        AppContainer.validate_configs(dict_with_duplicate_names_in_same_key)

    dict_with_duplicate_names_in_different_key = {
        "key1": [
            {
                "name": "name1",
            },
        ],
        "key2": [
            {
                "name": "name1",
            }
        ],
    }
    with pytest.raises(ValueError):
        AppContainer.validate_configs(dict_with_duplicate_names_in_different_key)

    dict_with_no_duplicate_names = {
        "key1": [{"name": "name1"}, {"name": "name2"}],
        "key2": [
            {
                "name": "name3",
            },
            {
                "name": "name4",
            },
        ],
    }

    # no error expected
    AppContainer.validate_configs(dict_with_no_duplicate_names)


@pytest.fixture(name="app_container_object")
def get_app_container(persistent):
    """
    App container fixture
    """
    user = User()
    task_manager = TaskManager(user_id=user.id)
    return AppContainer(
        user=user,
        persistent=persistent,
        temp_storage=LocalTempStorage(),
        task_manager=task_manager,
        storage=LocalTempStorage(),
        app_config=app_container_config,
    )


def test_app_container(app_container_object):
    """
    Test app container functions
    """
    # verify that there's nothing in the cached map
    assert len(AppContainer.cached_instance_map) == 0

    with patch.object(
        AppContainer, "build_instance_map", wraps=app_container_object.build_instance_map
    ) as mock_build_instance_map:
        # verify that building once invokes build_instance_map
        instance_map = app_container_object.get_instance_map()
        assert len(instance_map) > 0
        assert len(AppContainer.cached_instance_map) == len(instance_map)
        assert mock_build_instance_map.call_count == 1

        # verify that calling build again doesn't invoke build_instance_map again
        # also verify that the cached_instance_map doesn't change in size
        instance_map = app_container_object.get_instance_map()
        assert len(AppContainer.cached_instance_map) == len(instance_map)
        assert mock_build_instance_map.call_count == 1
