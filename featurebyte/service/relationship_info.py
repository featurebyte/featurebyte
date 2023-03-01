"""
Relationship Info Service
"""

from bson import ObjectId

from featurebyte.models.relationship import RelationshipInfo
from featurebyte.schema.relationship_info import RelationshipInfoCreate, RelationshipInfoUpdate
from featurebyte.service.base_document import BaseDocumentService


class RelationshipInfoService(
    BaseDocumentService[RelationshipInfo, RelationshipInfoCreate, RelationshipInfoUpdate]
):
    """
    RelationshipInfoService class is responsible for keeping track of the relationship info of various types.
    """

    document_class = RelationshipInfo
