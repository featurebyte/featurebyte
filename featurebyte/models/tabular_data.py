"""
This module contains TabularData pseudo models
"""
from __future__ import annotations

from typing import Any, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import DataModel as BaseDataModel
from featurebyte.models.item_data import ItemDataModel

DataModel = Annotated[Union[EventDataModel, ItemDataModel], Field(discriminator="type")]


class TabularDataModel(BaseDataModel):
    """
    Pseudo Data class to support multiple data types
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(DataModel, kwargs)  # type: ignore[arg-type]
