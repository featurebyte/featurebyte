"""
SlowlyChangingView class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.api.view import View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logger import logger
from featurebyte.query_graph.node.generic import SCDBaseParameters


class SlowlyChangingViewColumn(ViewColumn):
    """
    SlowlyChangingViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class SlowlyChangingView(View):
    """
    SlowlyChangingView class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.SlowlyChangingView",
    )

    _series_class = SlowlyChangingViewColumn

    natural_key_column: str = Field(allow_mutation=False)
    surrogate_key_column: str = Field(allow_mutation=False)
    effective_timestamp_column: str = Field(allow_mutation=False)
    end_timestamp_column: Optional[str] = Field(allow_mutation=False)
    current_flag_column: Optional[str] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> Optional[str]:
        return self.effective_timestamp_column

    @classmethod
    @typechecked
    def from_slowly_changing_data(
        cls, slowly_changing_data: SlowlyChangingData
    ) -> SlowlyChangingView:
        """
        Construct an SlowlyChangingView object

        Parameters
        ----------
        slowly_changing_data : SlowlyChangingData
            object used to construct SlowlyChangingView object

        Returns
        -------
        SlowlyChangingView
            constructed SlowlyChangingView object
        """
        return cls.from_data(
            slowly_changing_data,
            natural_key_column=slowly_changing_data.natural_key_column,
            surrogate_key_column=slowly_changing_data.surrogate_key_column,
            effective_timestamp_column=slowly_changing_data.effective_timestamp_column,
            end_timestamp_column=slowly_changing_data.end_timestamp_column,
            current_flag_column=slowly_changing_data.current_flag_column,
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
            "natural_key_column",
            "surrogate_key_column",
            "effective_timestamp_column",
            "end_timestamp_column",
            "current_flag_column",
        ]

    def _get_additional_inherited_columns(self) -> set[str]:
        return {self.effective_timestamp_column}

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
                "natural_key_column": self.natural_key_column,
                "surrogate_key_column": self.surrogate_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
            }
        )
        return params

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ---------
        other_view: View
            the other view that we are joining with

        Raises
        ------
        JoinViewMismatchError
            raised when the other view is a slowly changing view
        """
        if isinstance(other_view, SlowlyChangingView):
            logger.error("columns from a SlowlyChangingView can’t be added to a SlowlyChangingView")
            raise JoinViewMismatchError

    def get_join_column(self) -> str:
        return self.natural_key_column

    def _get_common_scd_parameters(self) -> SCDBaseParameters:
        """
        Get parameters related to Slowly Changing Data (SCD)

        Returns
        -------
        SCDBaseParameters
        """
        return SCDBaseParameters(
            effective_timestamp_column=self.effective_timestamp_column,
            natural_key_column=self.natural_key_column,
            current_flag_column=self.current_flag_column,
            end_timestamp_column=self.end_timestamp_column,
        )

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:

        # When calling_view doesn't have the timestamp_column attribute, it means that it is a
        # DimensionView. It is invalid to join DimensionView with SlowlyChangingView on the right
        # side. A validation error would have been raised before reaching here.
        assert hasattr(calling_view, "timestamp_column") and isinstance(
            calling_view.timestamp_column, str
        )

        left_timestamp_column = calling_view.timestamp_column
        return {
            "scd_parameters": {
                "left_timestamp_column": left_timestamp_column,
                **self._get_common_scd_parameters().dict(),
            }
        }

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        return {
            "scd_parameters": {
                "offset": offset,
                **self._get_common_scd_parameters().dict(),
            }
        }
