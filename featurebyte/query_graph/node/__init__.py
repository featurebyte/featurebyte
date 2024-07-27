"""
Query graph node related classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import TYPE_CHECKING, Union

from pydantic import Field
from typing_extensions import Annotated

from featurebyte.common.model_util import construct_serialize_function
from featurebyte.common.path_util import import_submodules
from featurebyte.query_graph.node.base import NODE_TYPES, BaseNode

if TYPE_CHECKING:
    # use the BaseNode class during type checking
    Node = BaseNode
else:
    # during runtime, load all submodules to populate NODE_TYPES
    import_submodules(__name__)

    # use the Annotated type for pydantic model deserialization
    Node = Annotated[Union[tuple(NODE_TYPES)], Field(discriminator="type")]


# construct function for node deserialization
construct_node = construct_serialize_function(
    all_types=NODE_TYPES,
    annotated_type=Node,
    discriminator_key="type",
)
