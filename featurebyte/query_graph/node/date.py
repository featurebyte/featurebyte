"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal

from pydantic import BaseModel, Field

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode


class BaseDatetimeOpNode(BaseNode):
    """Base class for datetime operation node"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)


class DatetimeExtractNode(BaseDatetimeOpNode):
    """DatetimeExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: DatetimeSupportedPropertyType

    # FIXME: should we rename DT_EXTRACT to DATETIME_EXTRACT
    type: Literal[NodeType.DT_EXTRACT] = Field(NodeType.DT_EXTRACT, const=True)
    parameters: Parameters


class TimeDeltaExtractNode(BaseDatetimeOpNode):
    """TimeDeltaExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = Field(NodeType.TIMEDELTA_EXTRACT, const=True)
    parameters: Parameters


class DateDifference(BaseDatetimeOpNode):
    """DateDifference class"""

    type: Literal[NodeType.DATE_DIFF] = Field(NodeType.DATE_DIFF, const=True)


class TimeDelta(BaseDatetimeOpNode):
    """TimeDelta class"""

    # FIXME: should we rename TimeDelta to TO_TIMEDELTA?
    type: Literal[NodeType.TIMEDELTA] = Field(NodeType.TIMEDELTA, const=True)


class DateAdd(BaseDatetimeOpNode):
    """DateAdd class"""

    type: Literal[NodeType.DATE_ADD] = Field(NodeType.DATE_ADD, const=True)
