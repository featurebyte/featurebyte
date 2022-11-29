"""
List of Exceptions
"""
from __future__ import annotations

from typing import Any

from requests.exceptions import JSONDecodeError
from requests.models import Response


class ResponseException(Exception):
    """
    Exception raised due to request handling failure
    """

    def __init__(self, response: Response, *args: Any, **kwargs: Any) -> None:
        self.response = response
        resolution = None
        if "resolution" in kwargs:
            resolution = kwargs.pop("resolution")
        exc_info = None
        try:
            response_dict = response.json()
            for field in ["detail", "traceback"]:
                if field in response_dict:
                    exc_info = response_dict[field]
                    break
            if resolution:
                exc_info += resolution
        except JSONDecodeError:
            exc_info = response.text

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


class TableSchemaHasBeenChangedError(Exception):
    """
    Raise when the table schema has been changed (different from the time EventData object is saved)
    """


class CredentialsError(Exception):
    """
    Raise when the credentials used to access the resource is missing or invalid
    """


class DocumentError(Exception):
    """
    General exception raised when there are some issue at persistent layer operations
    """


class DocumentNotFoundError(DocumentError):
    """
    Raise when the persistent query return emtpy result
    """


class DocumentConflictError(DocumentError):
    """
    Raise when there exists a conflicting document at the persistent
    """


class DocumentUpdateError(DocumentError):
    """
    Raise when the document invalid update happens
    """


class DocumentInconsistencyError(DocumentError):
    """
    Raise when the document consistency issue is detected
    """


class GraphInconsistencyError(DocumentError):
    """
    Raise when the graph consistency issue is detected
    """


class QueryExecutionTimeOut(DocumentError):
    """
    Raise when the SQL query execution times out
    """


class FeatureStoreSchemaCollisionError(Exception):
    """
    Raise when the feature store ID is already in use by another
    working schema.
    """


class NoFeatureStorePresentError(Exception):
    """
    Raise when we cannot find a feature store, when we expect one to be there.
    """


class JoinViewMismatchError(Exception):
    """
    Raise when the view types in a join are a mismatch.

    This ccould occur when
    - columns from a SCD View are trying to be added to a Dimension or SCD View. This operation is not allowed.
    - the target view to be joined with is not a SlowlyChangingView, or a DimensionView.
    """


class NoJoinKeyFoundError(Exception):
    """
    Raise when no suitable join key is found.

    This most likely indicates that callers should explicitly specify a join key.
    """


class RepeatedColumnNamesError(Exception):
    """
    Raise when two views have overlapping columns, and a user is trying to perform a Join, without providing
    a suffix
    """
