"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import Optional, Union
from typing_extensions import Annotated

from datetime import datetime  # pylint: disable=wrong-import-order

import pymongo
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.models.request_input import SourceTableRequestInput, ViewRequestInput


class ViewObservationInput(ViewRequestInput):
    """
    ViewObservationInput is a ViewRequestInput that is used to create an ObservationTableModel
    """


class SourceTableObservationInput(SourceTableRequestInput):
    """
    SourceTableObservationInput is a SourceTableRequestInput that is used to create an ObservationTableModel
    """


ObservationInput = Annotated[
    Union[ViewObservationInput, SourceTableObservationInput], Field(discriminator="type")
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
