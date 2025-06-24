"""
List of Exceptions
"""

from __future__ import annotations

from asyncio.exceptions import CancelledError
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


class OperationNotSupportedError(NotImplementedError):
    """
    Raise when the operation is not supported
    """


class BaseUnprocessableEntityError(FeatureByteException):
    """
    Base exception class for 422 Unprocessable Entity responses
    """


class BaseFailedDependencyError(FeatureByteException):
    """
    Base exception class for 424 Failed Dependency responses
    """


class BaseConflictError(FeatureByteException):
    """
    Base exception class for 409 Conflict responses
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


class RequiredEntityNotProvidedError(BaseUnprocessableEntityError):
    """
    Raised when one or more required entities are not provided
    """

    def __init__(self, *args: Any, repackaged: bool = False, **kwargs: Any) -> None:
        self.missing_entity_ids = kwargs.pop("missing_entity_ids", None)
        super().__init__(*args, repackaged=repackaged, **kwargs)


class UnexpectedServingNamesMappingError(BaseUnprocessableEntityError):
    """
    Raised when unexpected keys are provided in serving names mapping
    """


class InvalidSettingsError(FeatureByteException):
    """
    Raised when configuration has invalid settings
    """


class ObjectHasBeenSavedError(FeatureByteException):
    """
    Raise when the object has been saved before
    """


class CredentialsError(BaseUnprocessableEntityError):
    """
    Raise when the credentials used to access the resource is missing or invalid
    """


class DataWarehouseConnectionError(BaseUnprocessableEntityError):
    """
    Raise when connection to data warehouse cannot be established
    """


class DataWarehouseOperationError(BaseUnprocessableEntityError):
    """
    Raise when data warehouse operations failed
    """


class DocumentError(BaseUnprocessableEntityError):
    """
    General exception raised when there are some issue at persistent layer operations
    """


class DocumentNotFoundError(DocumentError):
    """
    Raise when the persistent query return emtpy result
    """


class DocumentConflictError(BaseConflictError):
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


class TimeOutError(FeatureByteException):
    """
    Time Out related exception
    """


class QueryExecutionTimeOut(TimeOutError):
    """
    Raise when the SQL query execution times out
    """


class SessionInitializationTimeOut(TimeOutError):
    """
    Raise when the session initialization times out
    """


class FeatureStoreSchemaCollisionError(BaseConflictError):
    """
    Raise when the feature store ID is already in use by another
    working schema.
    """

    def __str__(self) -> str:
        return "Feature Store ID is already in use."


class DeploymentNotEnabledError(FeatureByteException):
    """
    Raise when features are requested for a deployment that is not enabled
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


class CleaningOperationError(ValueError):
    """
    Raise when cleaning operation fails
    """


class TargetFillValueNotProvidedError(ValueError):
    """
    Raise when target fill value is not provided
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


class DeploymentDataBricksAccessorError(FeatureByteException):
    """
    Raise when there is an error with deployment
    """


class NoChangesInFeatureVersionError(DocumentError):
    """
    Raise when we try to create a new feature version, but there are no differences.
    """


class EntityRelationshipConflictError(DocumentError):
    """
    Raise when the entity relationship is conflicting with existing ones
    """


class LimitExceededError(BaseUnprocessableEntityError):
    """
    Raised when limit is exceeded
    """


class ColumnNotFoundError(FeatureByteException):
    """
    Raised when a specified column is not found in the view or table
    """


class FeatureMaterializationError(FeatureByteException):
    """
    Raised when there is an error with feature materialization
    """


class DockerError(FeatureByteException):
    """
    Raised when there is an error with Docker
    """


class DatabaseNotFoundError(BaseFailedDependencyError):
    """
    Raise when the requested database does not exist in the data warehouse
    """

    def __str__(self) -> str:
        return "Database not found. Please specify a valid database name."


class SchemaNotFoundError(BaseFailedDependencyError):
    """
    Raise when the requested schema does not exist in the data warehouse
    """

    def __str__(self) -> str:
        return "Schema not found. Please specify a valid schema name."


class TableNotFoundError(BaseFailedDependencyError):
    """
    Raise when the requested table does not exist in the data warehouse
    """

    def __str__(self) -> str:
        return "Table not found. Please specify a valid table name."


class CatalogNotSpecifiedError(BaseFailedDependencyError):
    """
    Raise when the catalog is not specified in a catalog-specific request
    """

    def __str__(self) -> str:
        return "Catalog not specified. Please specify a catalog."


class NotInDataBricksEnvironmentError(BaseFailedDependencyError):
    """
    Raise when the code is not running in a DataBricks environment
    """

    def __str__(self) -> str:
        return "This method can only be called in a DataBricks environment."


class UseCaseInvalidDataError(BaseUnprocessableEntityError):
    """
    Raise when invalid observation table default is specified
    """


class ObservationTableInvalidContextError(BaseUnprocessableEntityError):
    """
    Raise when invalid observation table context is specified
    """


class ObservationTableInvalidUseCaseError(BaseUnprocessableEntityError):
    """
    Raise when invalid use_case_id is specified when updating observation table
    """


class UnsupportedRequestCodeTemplateLanguage(BaseUnprocessableEntityError):
    """
    Raise when the language for request code template is not supported
    """


class UnsupportedObservationTableUploadFileFormat(BaseUnprocessableEntityError):
    """
    Raise when the file format for observation table upload is not supported
    """


class EntityTaggingIsNotAllowedError(BaseUnprocessableEntityError):
    """ "
    Raise when entity tagging is not allowed for a specific columns,
    for example scd surrogate key
    """


class ObservationTableMissingColumnsError(BaseUnprocessableEntityError):
    """
    Raise when observation table is missing required columns
    """


class ObservationTableInvalidTargetNameError(BaseUnprocessableEntityError):
    """
    Raise when observation table specifies a target name that does not exist
    """


class ObservationTableTargetDefinitionExistsError(BaseUnprocessableEntityError):
    """
    Raise when observation table specifies a target name that already has a definition
    """


class TaskNotRevocableError(BaseUnprocessableEntityError):
    """
    Raise when task is not revocable
    """


class TaskNotRerunnableError(BaseUnprocessableEntityError):
    """
    Raise when task is not rerunnable
    """


class TaskNotFound(DocumentNotFoundError):
    """
    Raise when task is not found
    """


# Exceptions to catch to handle task revoke
TaskRevokeExceptions = (SystemExit, KeyboardInterrupt, RuntimeError, CancelledError)


class TaskCanceledError(FeatureByteException):
    """
    Raise when task is canceled
    """


class CursorSchemaError(FeatureByteException):
    """
    Raise when cursor schema is not as expected
    """


class TableValidationError(FeatureByteException):
    """
    Raise when table validation fails
    """


class InvalidOutputRowIndexError(FeatureByteException):
    """
    Raise when output row index is invalid
    """


class FeatureTableRequestInputNotFoundError(BaseUnprocessableEntityError):
    """
    Raise when request input is not found for the feature table
    """


class FeatureQueryExecutionError(FeatureByteException):
    """
    Raise when there is an error with feature query execution
    """


class DescribeQueryExecutionError(FeatureByteException):
    """
    Raise when stats compute query fails to execute even after batching attempt
    """


class CronFeatureJobSettingConversionError(FeatureByteException):
    """
    Raise when there is an error when converting a CronFeatureJobSetting to a FeatureJobSetting
    """


class InvalidDefaultFeatureJobSettingError(FeatureByteException):
    """
    Raise when the default feature job setting is invalid
    """


class DeploymentNotOnlineEnabledError(FeatureByteException):
    """
    Raise when online features are requested for a deployment that is not online enabled
    """


class FeatureStoreNotInCatalogError(BaseUnprocessableEntityError):
    """
    Raise when the feature store is not in the specified catalog
    """


class InvalidViewSQL(BaseUnprocessableEntityError):
    """
    Raise when the SQL for a view is invalid
    """


class InvalidTableNameError(BaseUnprocessableEntityError):
    """
    Raise when an invalid table name is provided
    """


class InvalidTableSchemaError(BaseUnprocessableEntityError):
    """
    Raise when a table with invalid schema is provided
    """
