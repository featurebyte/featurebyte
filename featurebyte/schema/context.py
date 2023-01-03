"""
Context API payload schema
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.context import ContextModel, TabularDataToColumnNamesMapping
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.base import PaginationMixin


class ContextCreate(FeatureByteBaseModel):
    """
    Context creation schema
    """

    entity_ids: List[PydanticObjectId]
    schema_at_inference: Optional[List[TabularDataToColumnNamesMapping]]


class ContextList(PaginationMixin):
    """
    Paginated list of context
    """

    data: List[ContextModel]


class ContextUpdate(FeatureByteBaseModel):
    """
    Context update schema
    """

    graph: Optional[QueryGraph]
