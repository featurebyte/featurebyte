"""
This file contains the error messages used in the featurebyte package that is used in multiple places.
"""

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.table import TableService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.use_case import UseCaseService

MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE = (
    "Cannot delete {table_name} Table {document_id} because it is referenced by "
    "{total_ref_documents} {ref_table_name} Table(s): {ref_document_ids}"
)


class ObservationTableDeleteValidator:
    """
    Observation Table Delete Validator
    """

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        target_table_service: TargetTableService,
        use_case_service: UseCaseService,
    ):
        self.observation_table_service = observation_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.target_table_service = target_table_service
        self.use_case_service = use_case_service

    async def check_delete_observation_table(
        self,
        observation_table_id: ObjectId,
    ) -> ObservationTableModel:
        """
        Check that the observation table is not referenced in historical features or target tables.

        Parameters
        ----------
        observation_table_id: ObjectId
            Document id to delete

        Returns
        -------
        ObservationTableModel

        Raises
        ------
        DocumentDeletionError
            If the document cannot be deleted
        """
        document = await self.observation_table_service.get_document(
            document_id=observation_table_id
        )
        reference_ids = [
            str(doc["_id"])
            async for doc in self.historical_feature_table_service.list_documents_as_dict_iterator(
                query_filter={"observation_table_id": document.id},
                projection={"_id": 1},
            )
        ]
        target_reference_ids = [
            str(doc["_id"])
            async for doc in self.target_table_service.list_documents_as_dict_iterator(
                query_filter={"observation_table_id": document.id},
                projection={"_id": 1},
            )
        ]

        # check if the document is associated with a use case
        use_case_ids = [
            str(doc["_id"])
            async for doc in self.use_case_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": document.use_case_ids}},
                projection={"_id": 1},
            )
        ]

        all_reference_ids = reference_ids + target_reference_ids + use_case_ids
        if all_reference_ids:
            table_names = []
            if reference_ids:
                table_names.append("Historical Feature")
            if target_reference_ids:
                table_names.append("Target")
            if use_case_ids:
                table_names.append("UseCase")
            raise DocumentDeletionError(
                MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE.format(
                    document_id=document.id,
                    table_name="Observation",
                    ref_table_name=", ".join(table_names),
                    ref_document_ids=all_reference_ids,
                    total_ref_documents=len(all_reference_ids),
                )
            )
        return document


async def check_delete_batch_request_table(
    batch_request_table_service: BatchRequestTableService,
    batch_feature_table_service: BatchFeatureTableService,
    document_id: ObjectId,
) -> BatchRequestTableModel:
    """
    Check delete batch request table given the document id & services

    Parameters
    ----------
    batch_request_table_service: BatchRequestTableService
        Batch request table service
    batch_feature_table_service: BatchFeatureTableService
        Batch feature table service
    document_id: ObjectId
        Document id to delete

    Returns
    -------
    BatchRequestTableModel

    Raises
    ------
    DocumentDeletionError
        If the document cannot be deleted
    """
    document = await batch_request_table_service.get_document(document_id=document_id)
    reference_ids = [
        str(doc["_id"])
        async for doc in batch_feature_table_service.list_documents_as_dict_iterator(
            query_filter={"batch_request_table_id": document.id},
            projection={"_id": 1},
        )
    ]
    if reference_ids:
        raise DocumentDeletionError(
            MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE.format(
                document_id=document.id,
                table_name="Batch Request",
                ref_table_name="Batch Feature",
                ref_document_ids=reference_ids,
                total_ref_documents=len(reference_ids),
            )
        )
    return document


async def check_delete_static_source_table(
    static_source_table_service: StaticSourceTableService,
    table_service: TableService,
    document_id: ObjectId,
) -> StaticSourceTableModel:
    """
    Check delete static source table given the document id & services

    Parameters
    ----------
    static_source_table_service: StaticSourceTableService
        Static source table service
    table_service: TableService
        Table service
    document_id: ObjectId
        Document id to delete

    Returns
    -------
    StaticSourceTableModel

    Raises
    ------
    DocumentDeletionError
        If the document cannot be deleted
    """
    document = await static_source_table_service.get_document(document_id=document_id)
    reference_ids = [
        str(doc["_id"])
        async for doc in table_service.list_documents_as_dict_iterator(
            query_filter={"tabular_source": document.location.model_dump()},
            projection={"_id": 1},
        )
    ]
    if reference_ids:
        raise DocumentDeletionError(
            MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE.format(
                document_id=document.id,
                table_name="Static Source",
                ref_table_name="Featurebyte",
                ref_document_ids=reference_ids,
                total_ref_documents=len(reference_ids),
            )
        )
    return document
