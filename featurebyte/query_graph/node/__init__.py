"""
Query graph node related classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import TYPE_CHECKING, Any, Union, cast
from typing_extensions import Annotated

from pydantic import Field, TypeAdapter

from featurebyte.common.model_util import get_type_to_class_map
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


# construct node class map for deserialization
NODE_CLASS_MAP = get_type_to_class_map(NODE_TYPES)


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
    node_class = NODE_CLASS_MAP.get(kwargs.get("type"))  # type: ignore
    if node_class is None:
        # use pydantic builtin version to throw validation error (slow due to pydantic V2 performance issue)
        node = TypeAdapter(Node).validate_python(kwargs)  # type: ignore
    else:
        # use internal method to avoid current pydantic V2 performance issue due to _core_utils.py:walk
        # https://github.com/pydantic/pydantic/issues/6768
        node = node_class(**kwargs)
    return cast(Node, node)
