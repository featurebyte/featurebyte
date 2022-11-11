"""
ItemView class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.item_data import ItemData
from featurebyte.api.view import View, ViewColumn
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """


class ItemView(View):
    """
    ItemView class
    """

    _series_class = ItemViewColumn

    event_id_column: str = Field(allow_mutation=False)
    item_id_column: str = Field(allow_mutation=False)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_item_data(cls, item_data: ItemData) -> ItemView:
        """
        Construct an ItemView object

        Parameters
        ----------
        item_data : ItemData
            ItemData object used to construct ItemView object

        Returns
        -------
        ItemView
            constructed ItemView object
        """
        return cls.from_data(
            item_data,
            event_id_column=item_data.event_id_column,
            item_id_column=item_data.item_id_column,
            event_data_id=item_data.event_data_id,
            default_feature_job_setting=item_data.default_feature_job_setting,
        )

    @property
    def additional_protected_attributes(self) -> list[str]:
        return ["event_id_column", "item_id_column"]

    @property
    def inherited_columns(self) -> set[str]:
        return set()

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "event_id_column": self.event_id_column,
                "item_id_column": self.item_id_column,
                "event_data_id": self.event_data_id,
            }
        )
        return params
