"""
This file contains the error messages used in the featurebyte package that is used in multiple places.
"""
from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService

MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE = (
    "Cannot delete {table_name} Table {document_id} because it is referenced by "
    "{total_ref_documents} {ref_table_name} Table(s): {ref_document_ids}"
)


async def check_delete_observation_table(
    observation_table_service: ObservationTableService,
    historical_feature_table_service: HistoricalFeatureTableService,
    document_id: ObjectId,
) -> ObservationTableModel:
    """
    Check delete observation table given the document id & services

    Parameters
    ----------
    observation_table_service: ObservationTableService
        Observation table service
    historical_feature_table_service: HistoricalFeatureTableService
        Historical feature table service
    document_id: ObjectId
        Document id to delete

    Returns
    -------
    ObservationTableModel

    Raises
    ------
    DocumentDeletionError
        If the document cannot be deleted
    """
    document = await observation_table_service.get_document(document_id=document_id)
    reference_ids = [
        str(doc["_id"])
        async for doc in historical_feature_table_service.list_documents_iterator(
            query_filter={"observation_table_id": document.id}
        )
    ]
    if reference_ids:
        raise DocumentDeletionError(
            MATERIALIZED_TABLE_DELETE_ERROR_MESSAGE.format(
                document_id=document.id,
                table_name="Observation",
                ref_table_name="Historical Feature",
                ref_document_ids=reference_ids,
                total_ref_documents=len(reference_ids),
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
        async for doc in batch_feature_table_service.list_documents_iterator(
            query_filter={"batch_request_table_id": document.id}
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
