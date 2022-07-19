"""
List of Exceptions
"""
from __future__ import annotations

from typing import Any

from requests.models import Response


class ResponseException(Exception):
    """
    Exception raised due to request handling failure
    """

    def __init__(self, response: Response, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.response = response

    @property
    def text(self) -> str:
        """
        Response text
        Returns
        -------
        str
        """
        return self.response.text

    @property
    def status_code(self) -> int:
        """
        Response status code
        Returns
        -------
        int
        """
        return self.response.status_code


class RecordCreationException(ResponseException):
    """
    General failure during creating an object to persistent layer
    """


class DuplicatedRecordException(ResponseException):
    """
    Failure creation at persistent layer due to conflict record
    """


class RecordUpdateException(ResponseException):
    """
    General failure during updating an existing record at persistent layer
    """


class RecordRetrievalException(ResponseException):
    """
    General failure during retrieving an existing record at persistent layer
    """


class MissingPointInTimeColumnError(Exception):
    """
    Raised when point in time column is not provided in historical requests
    """


class TooRecentPointInTimeError(Exception):
    """
    Raised when the latest point in time value is too recent in historical requests
    """


class InvalidSettingsError(Exception):
    """
    Raised when configuration has invalid settings
    """


class DuplicatedFeatureRegistryError(Exception):
    """
    Raised when the feature registry record already exists at the feature store
    """


class MissingFeatureRegistryError(Exception):
    """
    Raised when the feature registry record does not exist
    """


class InvalidFeatureRegistryOperationError(Exception):
    """
    Raised when the operation on the registry is invalid
    """
