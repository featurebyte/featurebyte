"""
Primary entity validator
"""

from typing import List

from featurebyte.models.base import PydanticObjectId
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.service.entity import EntityService


class PrimaryEntityValidator:
    """
    Primary entity validator
    """

    def __init__(
        self,
        entity_service: EntityService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        self.entity_service = entity_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    async def validate_entities_are_primary_entities(
        self, entity_ids: List[PydanticObjectId]
    ) -> None:
        """
        Validate that all entities provided in the list of entity IDs are primary entities.

        Parameters
        ----------
        entity_ids: List[PydanticObjectId]
            List of entity IDs to be validated

        Raises
        ------
        ValueError
            When any of the entity IDs is not a primary entity ID
        """

        # validate entity ids exist and each entity id's parents must not be in the entity ids
        all_parents_ids = []
        for entity_id in entity_ids:
            entity = await self.entity_service.get_document(document_id=entity_id)
            for parent in entity.parents:
                all_parents_ids.append(parent.id)

        if set(all_parents_ids).intersection(set(entity_ids)):
            raise ValueError("Entity ids must not include any parent entity ids")

        # validate all entity ids are primary entity ids
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=entity_ids
        )
        if set(primary_entity_ids) != set(entity_ids):
            not_primary = set(entity_ids).difference(set(primary_entity_ids))
            raise ValueError(
                f"Entity ids must all be primary entity ids: {[str(x) for x in not_primary]}"
            )
