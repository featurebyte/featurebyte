"""
Query graph node related classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Union
from typing_extensions import Annotated

from pydantic import Field

from featurebyte.query_graph.node.binary import (
    AddNode,
    AndNode,
    DivideNode,
    EqualNode,
    GreaterEqualNode,
    GreaterThanNode,
    LessEqualNode,
    LessThanNode,
    ModuloNode,
    MultiplyNode,
    NotEqualNode,
    OrNode,
    PowerNode,
    SubtractNode,
)
from featurebyte.query_graph.node.count_dict import CosineSimilarityNode, CountDictTransformNode
from featurebyte.query_graph.node.date import (
    DateAdd,
    DateDifference,
    DatetimeExtractNode,
    TimeDelta,
    TimeDeltaExtractNode,
)
from featurebyte.query_graph.node.sql import (
    AliasNode,
    AssignNode,
    CastNode,
    ConditionalNode,
    FilterNode,
    GroupbyNode,
    InputNode,
    LagNode,
    ProjectNode,
)
from featurebyte.query_graph.node.string import (
    ConcatNode,
    LengthNode,
    PadNode,
    ReplaceNode,
    StringCaseNode,
    StringContainsNode,
    SubStringNode,
    TrimNode,
)
from featurebyte.query_graph.node.unary import (
    AbsoluteNode,
    CeilNode,
    ExponentialNode,
    FloorNode,
    IsNullNode,
    LogNode,
    NotNode,
    SquareRootNode,
)

Node = Annotated[
    Union[
        AbsoluteNode,
        AddNode,
        AliasNode,
        AndNode,
        AssignNode,
        CastNode,
        CeilNode,
        ConcatNode,
        ConditionalNode,
        CosineSimilarityNode,
        CountDictTransformNode,
        DateAdd,
        DateDifference,
        DatetimeExtractNode,
        DivideNode,
        EqualNode,
        ExponentialNode,
        FilterNode,
        FloorNode,
        GreaterEqualNode,
        GreaterThanNode,
        GroupbyNode,
        InputNode,
        IsNullNode,
        LagNode,
        LengthNode,
        LessEqualNode,
        LessThanNode,
        LogNode,
        ModuloNode,
        MultiplyNode,
        NotEqualNode,
        NotNode,
        OrNode,
        PadNode,
        PowerNode,
        ProjectNode,
        ReplaceNode,
        SquareRootNode,
        StringCaseNode,
        StringContainsNode,
        SubStringNode,
        SubtractNode,
        TimeDelta,
        TimeDeltaExtractNode,
        TrimNode,
    ],
    Field(discriminator="type"),
]
