"""
Context API payload schema
"""

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class ContextCreate(FeatureByteBaseModel):
    """
    Context creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    primary_entity_ids: List[PydanticObjectId]
    description: Optional[StrictStr]


class ContextList(PaginationMixin):
    """
    Paginated list of context
    """

    data: List[ContextModel]


class ContextUpdate(BaseDocumentServiceUpdateSchema):
    """
    Context update schema
    """

    graph: Optional[QueryGraph]
    node_name: Optional[StrictStr]

    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]
    observation_table_id_to_remove: Optional[PydanticObjectId]

    remove_default_eda_table: Optional[bool]
    remove_default_preview_table: Optional[bool]

    name: Optional[NameStr]

    @root_validator(pre=True)
    @classmethod
    def _validate_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # check xor between graph & node_name
        graph = values.get("graph")
        node_name = values.get("node_name")
        if bool(graph) != bool(node_name):
            raise ValueError("graph & node_name parameters must be specified together.")
        if graph:
            if node_name not in QueryGraph(**dict(graph)).nodes_map:
                raise ValueError("node_name not exists in the graph.")

        # check for default_preview_table_id and default_eda_table_id against observation_table_id_to_remove
        default_preview_table_id = values.get("default_preview_table_id", None)
        default_eda_table_id = values.get("default_eda_table_id", None)
        observation_table_id_to_remove = values.get("observation_table_id_to_remove", None)

        if observation_table_id_to_remove:
            if (
                default_preview_table_id
                and default_preview_table_id == observation_table_id_to_remove
            ):
                raise ValueError(
                    "observation_table_id_to_remove cannot be the same as default_preview_table_id"
                )

            if default_eda_table_id and default_eda_table_id == observation_table_id_to_remove:
                raise ValueError(
                    "observation_table_id_to_remove cannot be the same as default_eda_table_id"
                )

        return values
