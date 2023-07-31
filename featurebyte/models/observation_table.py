"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import Optional, Union
from typing_extensions import Annotated, Literal

from datetime import datetime  # pylint: disable=wrong-import-order

import pymongo
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.models.request_input import (
    RequestInputType,
    SourceTableRequestInput,
    ViewRequestInput,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base import BaseSession


class ViewObservationInput(ViewRequestInput):
    """
    ViewObservationInput is a ViewRequestInput that is used to create an ObservationTableModel
    """


class SourceTableObservationInput(SourceTableRequestInput):
    """
    SourceTableObservationInput is a SourceTableRequestInput that is used to create an ObservationTableModel
    """


class TargetInput(FeatureByteBaseModel):
    """
    TargetInput is an input from a target that can eb used to create an ObservationTableModel
    """

    target_id: PydanticObjectId
    observation_table_id: Optional[PydanticObjectId]
    type: Literal[RequestInputType.OBSERVATION_TABLE, RequestInputType.DATAFRAME]

    async def materialize(
        self,
        session: BaseSession,
        destination: TableDetails,
        sample_rows: Optional[int],
    ) -> None:
        """
        No-op materialize. This method isn't needed for TargetInput since we materialize the target separately.
        As such, we can add a no-op version that throws an error if it is called. This is needed to satisfy the
        linter since some of the other classes under the `ObservationInput` union type have a materialize method.
        Will consider refactoring this in the future.

        Parameters
        ----------
        session: BaseSession
            The session to use to materialize the target input
        destination: TableDetails
            The destination table to materialize the target input to
        sample_rows: Optional[int]
            The number of rows to sample from the target input

        Raises
        ------
        NotImplementedError
            If this method is called
        """
        raise NotImplementedError


ObservationInput = Annotated[
    Union[ViewObservationInput, SourceTableObservationInput, TargetInput],
    Field(discriminator="type"),
]


class ObservationTableModel(MaterializedTableModel):
    """
    ObservationTableModel is a table that can be used to request historical features

    request_input: ObservationInput
        The input that defines how the observation table is created
    context_id: Optional[PydanticObjectId]
        The id of the context that the observation table is associated with
    """

    request_input: ObservationInput
    most_recent_point_in_time: StrictStr
    context_id: Optional[PydanticObjectId] = Field(default=None)

    @validator("most_recent_point_in_time")
    @classmethod
    def _validate_most_recent_point_in_time(cls, value: str) -> str:
        # Check that most_recent_point_in_time is a valid ISO 8601 datetime
        _ = datetime.fromisoformat(value)
        return value

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "observation_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
