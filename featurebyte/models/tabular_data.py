"""
This module contains TabularData pseudo models.
"""
from __future__ import annotations

from typing import Any, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import DataModel as BaseDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.models.scd_data import SCDDataModel

DataModel = Annotated[
    Union[EventDataModel, ItemDataModel, DimensionDataModel, SCDDataModel],
    Field(discriminator="type"),
]


class TabularDataModel(BaseDataModel):  # pylint: disable=abstract-method
    """
    Pseudo Data class to support multiple data types.
    This class basically parses the persistent data model record & deserialized it into proper type.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(DataModel, kwargs)  # type: ignore[arg-type]
