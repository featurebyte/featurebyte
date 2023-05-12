"""
Test docker manager
"""
from unittest.mock import patch

import pytest
from python_on_whales import DockerException

from featurebyte.docker.manager import start_playground
from featurebyte.exception import DockerError


@patch("featurebyte.docker.manager.DockerClient.__new__")
def test_docker_service_not_found(mock_docker_client_new):
    """
    Test docker service not found raise informative featurebyte exception
    """
    mock_docker_client_new.side_effect = DockerException(
        command_launched=["docker", "network", "ls"],
        return_code=1,
        stderr=b"Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running",
    )
    # expect docker error with informative message
    with pytest.raises(DockerError) as exc:
        start_playground()
    assert "Docker is not running, please start docker and try again." in str(exc.value)


@patch("featurebyte.docker.manager.DockerClient.__new__")
def test_docker_service_error(mock_docker_client_new):
    """
    Test docker service error raise informative featurebyte exception
    """
    docker_client = mock_docker_client_new.return_value
    docker_client.network.list.side_effect = DockerException(
        command_launched=["docker", "network", "ls"],
        return_code=1,
        stderr=b"Some docker error",
    )
    # expect docker error with informative message
    with pytest.raises(DockerError) as exc:
        start_playground()
    assert "Failed to start application: Some docker error" in str(exc.value)
