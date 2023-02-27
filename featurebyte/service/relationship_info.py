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

    async def update_relationship_info(self, relationship_id: ObjectId, enable: bool) -> None:
        """
        Update relationship enable

        Parameters
        ----------
        relationship_id: ObjectId
            The relationship id
        enable: bool
            The enable value
        """
        await self.persistent.update_one(
            collection_name="relationship_info",
            query_filter={"_id": {"$in": [relationship_id]}},
            update={"$set": {"is_enabled": enable}},
            user_id=self.user.id,
        )
