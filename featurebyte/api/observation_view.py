"""
ObservationView class
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field

from featurebyte.api.view import View
from featurebyte.core.frame import Frame
from featurebyte.core.generic import QueryObject


class ObservationViewMixin:
    """
    ObservationViewMixin class provides a method to create an ObservationView from a View
    """

    def get_observation_view(self: View) -> ObservationView:  # type: ignore
        """
        Create an ObservationView from the View.

        Returns
        -------
        ObservationView
            The created ObservationView object.
        """
        if hasattr(self, "timestamp_column"):
            point_in_time_column = self.timestamp_column
        else:
            point_in_time_column = None
        return ObservationView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=self.columns_info,
            node_name=self.node_name,
            point_in_time_column=point_in_time_column,
        )


class ObservationView(Frame, QueryObject):
    """
    ObservationView can be used to construct observations set for requesting features
    """

    point_in_time_column: Optional[str] = Field(default=None, allow_mutation=False)
