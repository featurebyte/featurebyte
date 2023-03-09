"""
Unit tests for feature/exception.py module
"""
import json

import pytest
from requests import Response

from featurebyte.exception import ResponseException


def test_response_exception__empty_text_response():
    """Test response exception on empty text response"""
    response = Response()
    response_exception = ResponseException(response=response)
    assert response_exception.response == response
    assert response_exception.text == ""
    assert response_exception.status_code is None


def test_response_exception__json_text_response():
    """Test response exception on json text response"""
    response = Response()
    response._content = bytes(json.dumps({"detail": "some error"}), "utf-8")
    response.status_code = 500
    response_exception = ResponseException(response=response)
    assert str(response_exception) == "some error"
    assert response_exception.text == '{"detail": "some error"}'
    assert response_exception.status_code == 500


def test_response_exception__plain_text_response():
    """Test response exception for plain text 500 response"""
    response = Response()
    response._content = bytes("some error", "utf-8")
    response.status_code = 500
    response_exception = ResponseException(response=response)
    assert str(response_exception) == "some error"
    assert response_exception.text == "some error"
    assert response_exception.status_code == 500


@pytest.mark.parametrize("detail", ["Some details.", ["Some details."], ["Some", "details."]])
def test_response_exception__response_detail(detail):
    """Test response exception for plain text response"""
    response = Response()
    response._content = json.dumps({"detail": detail}).encode("utf-8")
    response_exception = ResponseException(
        response=response,
        resolution=(
            "\nIf the error is related to connection broken, "
            "try to use a smaller `max_batch_size` parameter."
        ),
    )
    with pytest.raises(ResponseException) as exc:
        raise response_exception

    expected = (
        "Some details.\n"
        "If the error is related to connection broken, try to use a smaller `max_batch_size` parameter."
    )
    assert expected in str(exc.value)
