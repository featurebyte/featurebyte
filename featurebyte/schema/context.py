"""
Context API payload schema
"""
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class ContextCreate(FeatureByteBaseModel):
    """
    Context creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    entity_ids: List[PydanticObjectId]


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

    @root_validator()
    @classmethod
    def _validate_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # check xor between graph & node_name
        if bool(values["graph"]) != bool(values["node_name"]):
            raise ValueError("graph & node_name parameters must be specified together.")
        return values
