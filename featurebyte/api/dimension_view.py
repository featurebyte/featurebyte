"""
DimensionView class
"""
from __future__ import annotations

from typing import Any

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.view import View, ViewColumn


class DimensionViewColumn(ViewColumn):
    """
    DimensionViewColumn class
    """


class DimensionView(View):
    """
    DimensionView class
    """

    _series_class = DimensionViewColumn

    dimension_data_id_column: str = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_dimension_data(cls, dimension_data: DimensionData) -> DimensionView:
        """
        Construct an DimensionView object

        Parameters
        ----------
        dimension_data : DimensionData
            object used to construct DimensionView object

        Returns
        -------
        DimensionView
            constructed DimensionView object
        """
        return cls.from_data(
            dimension_data,
            dimension_data_id_column=dimension_data.dimension_data_id_column,
        )

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + [
            "dimension_data_id_column",
        ]

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
                "dimension_data_id_column": self.dimension_data_id_column,
            }
        )
        return params
