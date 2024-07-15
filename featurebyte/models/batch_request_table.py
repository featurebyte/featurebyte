"""
BatchRequestTable models
"""

from __future__ import annotations

from typing import Optional, Union

import pymongo
from pydantic import Field
from typing_extensions import Annotated

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.models.request_input import SourceTableRequestInput, ViewRequestInput


class ViewBatchRequestInput(ViewRequestInput):
    """
    ViewBatchRequestInput is a ViewRequestInput that is used to create a BatchRequestTable
    """


class SourceTableBatchRequestInput(SourceTableRequestInput):
    """
    SourceTableBatchRequestInput is a SourceTableRequestInput that is used to create a BatchRequestTable
    """


BatchRequestInput = Annotated[
    Union[ViewBatchRequestInput, SourceTableBatchRequestInput], Field(discriminator="type")
]


class BatchRequestTableModel(MaterializedTableModel):
    """
    BatchRequestTableModel represents a table that can be used to request online feature requests

    request_input: BatchRequestInput
        The input that defines how the batch request table is created
    context_id: Optional[PydanticObjectId]
        The id of the context that the batch request table is associated with
    """

    request_input: BatchRequestInput
    context_id: Optional[PydanticObjectId] = Field(default=None)

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "batch_request_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
