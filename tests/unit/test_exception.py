"""
Unit tests for feature/exception.py module
"""
import json

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
