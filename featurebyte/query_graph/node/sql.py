"""
This module contains SQL operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode, InColumnStr, OutColumnStr


class InputNode(BaseNode):
    """InputNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        columns: List[InColumnStr]
        dbtable: TableDetails
        feature_store: FeatureStoreModel
        timestamp: InColumnStr

    type: Literal[NodeType.INPUT] = Field(NodeType.INPUT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters


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
    parameters: BaseModel = Field(default_factory=BaseModel, const=True)


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

        value: Optional[Union[int, float, str, bool]]

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class AliasNode(BaseNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        # FIXME: should we include input column name also?
        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.CONDITIONAL, const=True)
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
