"""
List of Exceptions
"""
from __future__ import annotations

from typing import Any

from requests.exceptions import JSONDecodeError
from requests.models import Response


class FeatureByteException(Exception):
    """
    Base exception class for FeatureByte related exceptions.
    Use this class as a base class for all exceptions raised by FeatureByte SDK so that
    they can be repackaged for readability in a notebook environment.
    """

    def __init__(self, *args: Any, repackaged: bool = False, **kwargs: Any) -> None:
        self.repackaged = repackaged
        super().__init__(*args, **kwargs)


class ResponseException(FeatureByteException):
    """
    Exception raised due to request handling failure
    """

    def __init__(self, response: Response, *args: Any, **kwargs: Any) -> None:
        self.response = response
        resolution = None
        if "resolution" in kwargs:
            resolution = kwargs.pop("resolution")
        exc_info = response.text
        try:
            response_dict = response.json()
            field_value: Any = None
            for field in ["detail", "traceback"]:
                if field in response_dict:
                    field_value = response_dict[field]
                    break

            if isinstance(field_value, list):
                exc_info = " ".join(str(elem) for elem in field_value)
            elif field_value:
                exc_info = str(field_value)
        except JSONDecodeError:
            pass

        if resolution:
            exc_info += resolution

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


class QueryNotSupportedError(NotImplementedError):
    """
    Raise when the persistent query is not supported
    """


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


class RecordDeletionException(ResponseException):
    """
    General failure during deleting an existing record at persistent layer
    """


class MissingPointInTimeColumnError(FeatureByteException):
    """
    Raised when point in time column is not provided
    """


class UnsupportedPointInTimeColumnTypeError(FeatureByteException):
    """
    Raised when point in time column type is not supported
    """


class TooRecentPointInTimeError(FeatureByteException):
    """
    Raised when the latest point in time value is too recent in historical requests
    """


class RequiredEntityNotProvidedError(FeatureByteException):
    """
    Raised when one or more required entities are not provided
    """


class UnexpectedServingNamesMappingError(FeatureByteException):
    """
    Raised when unexpected keys are provided in serving names mapping
    """


class EntityJoinPathNotFoundError(FeatureByteException):
    """
    Raised when it is not possible to identify a join path to an entity using the provided entities
    as children entities
    """


class AmbiguousEntityRelationshipError(FeatureByteException):
    """
    Raised when the relationship between entities is ambiguous and automatic serving of parent
    features is not possible
    """


class InvalidSettingsError(FeatureByteException):
    """
    Raised when configuration has invalid settings
    """


class DuplicatedRegistryError(FeatureByteException):
    """
    Raised when the feature registry record already exists at the feature store
    """


class MissingFeatureRegistryError(FeatureByteException):
    """
    Raised when the feature registry record does not exist
    """


class InvalidFeatureRegistryOperationError(FeatureByteException):
    """
    Raised when the operation on the registry is invalid
    """


class ObjectHasBeenSavedError(FeatureByteException):
    """
    Raise when the object has been saved before
    """


class TableSchemaHasBeenChangedError(FeatureByteException):
    """
    Raise when the table schema has been changed (different from the time EventTable object is saved)
    """


class CredentialsError(FeatureByteException):
    """
    Raise when the credentials used to access the resource is missing or invalid
    """


class DocumentError(FeatureByteException):
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


class DocumentCreationError(DocumentError):
    """
    Raise when the document invalid creation happens
    """


class DocumentUpdateError(DocumentError):
    """
    Raise when the document invalid update happens
    """


class DocumentDeletionError(DocumentError):
    """
    Raise when the document invalid deletion happens
    """


class DocumentInconsistencyError(DocumentError):
    """
    Raise when the document consistency issue is detected
    """


class DocumentModificationBlockedError(DocumentError):
    """
    Raise when the document modification is blocked
    """


class GraphInconsistencyError(DocumentError):
    """
    Raise when the graph consistency issue is detected
    """


class QueryExecutionTimeOut(DocumentError):
    """
    Raise when the SQL query execution times out
    """


class FeatureStoreSchemaCollisionError(FeatureByteException):
    """
    Raise when the feature store ID is already in use by another
    working schema.
    """


class NoFeatureStorePresentError(FeatureByteException):
    """
    Raise when we cannot find a feature store, when we expect one to be there.
    """


class FeatureListNotOnlineEnabledError(FeatureByteException):
    """
    Raise when online features are requested for a FeatureList that is not online enabled
    """


class JoinViewMismatchError(FeatureByteException):
    """
    Raise when the view types in a join are a mismatch.

    This ccould occur when
    - columns from a SCD View are trying to be added to a Dimension or SCD View. This operation is not allowed.
    - the target view to be joined with is not a SCDView, or a DimensionView.
    """


class NoJoinKeyFoundError(FeatureByteException):
    """
    Raise when no suitable join key is found.

    This most likely indicates that callers should explicitly specify a join key.
    """


class RepeatedColumnNamesError(FeatureByteException):
    """
    Raise when two views have overlapping columns, and a user is trying to perform a Join, without providing
    a suffix
    """


class AggregationNotSupportedForViewError(FeatureByteException):
    """
    Raise when the requested aggregation does not support the underlying View
    """


class InvalidImputationsError(ValueError):
    """
    Raise when the imputations do not fulfill constraints (for example, double imputations is detected).
    """


class EventViewMatchingEntityColumnNotFound(FeatureByteException):
    """
    Raise when we are unable to find a matching entity column when trying to add a feature to an event view.
    """


class ChangeViewNoJoinColumnError(FeatureByteException):
    """
    Raise when get_join_column is called in ChangeView.

    ChangeView's don't have a primary key, and as such we don't expect there to be a join column.
    """


class NoFeatureJobSettingInSourceError(FeatureByteException):
    """
    Raise when the input table does not have any feature job setting.
    """


class NoChangesInFeatureVersionError(DocumentError):
    """
    Raise when we try to create a new feature version, but there are no differences.
    """


class LimitExceededError(FeatureByteException):
    """
    Raised when limit is exceeded
    """


class ColumnNotFoundError(FeatureByteException):
    """
    Raised when a specified column is not found in the view or table
    """


class DockerError(FeatureByteException):
    """
    Raised when there is an error with Docker
    """


class DatabaseNotFoundError(FeatureByteException):
    """
    Raise when the requested database does not exist in the data warehouse
    """


class SchemaNotFoundError(FeatureByteException):
    """
    Raise when the requested schema does not exist in the data warehouse
    """


class TableNotFoundError(FeatureByteException):
    """
    Raise when the requested table does not exist in the data warehouse
    """


class CatalogNotSpecifiedError(FeatureByteException):
    """
    Raise when the catalog is not specified in a catalog-specific request
    """
