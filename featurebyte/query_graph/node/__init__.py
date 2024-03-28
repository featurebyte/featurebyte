"""
Query graph node related classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import TYPE_CHECKING, Any, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as

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


def construct_node(**kwargs: Any) -> Node:
    """
    Construct node based on input keyword arguments

    Parameters
    ----------
    **kwargs: Any
        Keyword arguments used to construct the Node object

    Returns
    -------
    Node
    """
    node = parse_obj_as(Node, kwargs)  # type: ignore[misc]
    return node
