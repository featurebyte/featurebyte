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
        self.response = response
        response_dict = response.json()
        exc_info = None
        for field in ["detail", "traceback"]:
            if field in response_dict:
                exc_info = response_dict[field]
                break

        if exc_info:
            super().__init__(exc_info, *args, **kwargs)
        else:
            super().__init__(*args, **kwargs)

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


class MissingServingNameError(Exception):
    """
    Raised when one or more required serving names are not provided
    """


class InvalidSettingsError(Exception):
    """
    Raised when configuration has invalid settings
    """


class DuplicatedRegistryError(Exception):
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


class ObjectHasBeenSavedError(Exception):
    """
    Raise when the object has been saved before
    """
