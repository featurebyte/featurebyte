"""
This module contains SQL operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, root_validator

from featurebyte.enum import AggFunc, DBVarType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreDetails, TableDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode, InColumnStr, OutColumnStr


class InputNode(BaseNode):
    """InputNode class"""

    class BaseParameters(BaseModel):
        """BaseParameters"""

        columns: List[InColumnStr]
        table_details: TableDetails
        feature_store_details: FeatureStoreDetails
        id: Optional[PydanticObjectId] = Field(default=None)

    class EventDataParameters(BaseParameters):
        """EventDataParameters"""

        type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
        timestamp: Optional[InColumnStr]

        @root_validator(pre=True)
        @classmethod
        def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            # DEV-556: converted older record (parameters) into a newer format
            if "dbtable" in values:
                values["table_details"] = values["dbtable"]
            if "feature_store" in values:
                values["feature_store_details"] = values["feature_store"]
            return values

    type: Literal[NodeType.INPUT] = Field(NodeType.INPUT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: EventDataParameters


class ProjectNode(BaseNode):
    """ProjectNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        columns: List[InColumnStr]

    type: Literal[NodeType.PROJECT] = Field(NodeType.PROJECT, const=True)
    parameters: Parameters


class FilterNode(BaseNode):
    """FilterNode class"""

    type: Literal[NodeType.FILTER] = Field(NodeType.FILTER, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)


class AssignNode(BaseNode):
    """AssignNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr
        value: Optional[Any]

    type: Literal[NodeType.ASSIGN] = Field(NodeType.ASSIGN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters


class ConditionalNode(BaseNode):
    """ConditionalNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Any]

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class AliasNode(BaseNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        # FIXME: should we include input column name also?
        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.ALIAS, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class CastNode(BaseNode):
    """CastNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        type: Literal["int", "float", "str"]
        from_dtype: DBVarType

    type: Literal[NodeType.CAST] = Field(NodeType.CAST, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class LagNode(BaseNode):
    """LagNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        entity_columns: List[InColumnStr]
        timestamp_column: InColumnStr
        offset: int

    type: Literal[NodeType.LAG] = Field(NodeType.LAG, const=True)
    output_type = NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class GroupbyNode(BaseNode):
    """GroupbyNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        keys: List[InColumnStr]
        parent: Optional[InColumnStr]
        agg_func: AggFunc
        value_by: Optional[InColumnStr]
        windows: List[str]
        timestamp: InColumnStr
        blind_spot: int
        time_modulo_frequency: int
        frequency: int
        names: List[str]
        serving_names: List[str]
        tile_id: Optional[str]
        aggregation_id: Optional[str]

    type: Literal[NodeType.GROUPBY] = Field(NodeType.GROUPBY, const=True)
    parameters: Parameters
