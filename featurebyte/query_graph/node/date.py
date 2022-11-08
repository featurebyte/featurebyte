"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal, Optional

from pydantic import BaseModel, Field

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode


class DatetimeExtractNode(BaseSeriesOutputNode):
    """DatetimeExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: DatetimeSupportedPropertyType

    type: Literal[NodeType.DT_EXTRACT] = Field(NodeType.DT_EXTRACT, const=True)
    parameters: Parameters


class TimeDeltaExtractNode(BaseSeriesOutputNode):
    """TimeDeltaExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = Field(NodeType.TIMEDELTA_EXTRACT, const=True)
    parameters: Parameters


class DateDifference(BaseSeriesOutputNode):
    """DateDifference class"""

    type: Literal[NodeType.DATE_DIFF] = Field(NodeType.DATE_DIFF, const=True)


class TimeDelta(BaseSeriesOutputNode):
    """TimeDelta class"""

    class Parameters(BaseModel):
        """Parameters"""

        unit: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA] = Field(NodeType.TIMEDELTA, const=True)
    parameters: Parameters


class DateAdd(BaseSeriesOutputNode):
    """DateAdd class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[int]

    type: Literal[NodeType.DATE_ADD] = Field(NodeType.DATE_ADD, const=True)
    parameters: Parameters
