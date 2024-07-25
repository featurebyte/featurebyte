"""
Entity Relationship Extractor Service
"""

import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from bson import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship_info import RelationshipInfoService


@dataclass
class EntityRelationshipData:
    """
    Entity relationship data
    """

    entity_id: ObjectId
    relationship_id: ObjectId


@dataclass
class ServingEntityEnumeration:
    """
    Serving entity enumeration class is used to enumerate
    all serving entity IDs from the given entity IDs
    """

    entity_ancestor_descendant_mapper: EntityAncestorDescendantMapper

    @classmethod
    def create(cls, relationships_info: List[EntityRelationshipInfo]) -> "ServingEntityEnumeration":
        """
        Create serving entity enumeration from entity relationships info

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            List of entity relationships info

        Returns
        -------
        ServingEntityEnumeration
        """
        return cls(
            entity_ancestor_descendant_mapper=EntityAncestorDescendantMapper.create(
                relationships_info=relationships_info
            )
        )

    def reduce_entity_ids(self, entity_ids: Sequence[ObjectId]) -> List[ObjectId]:
        """
        Reduce entity IDs to only contain the given entity IDs that are not ancestors of any other entity IDs

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs

        Returns
        -------
        List[ObjectId]
        """
        return self.entity_ancestor_descendant_mapper.reduce_entity_ids(entity_ids=entity_ids)

    def generate(self, entity_ids: List[ObjectId]) -> List[List[ObjectId]]:
        """
        Enumerate all serving entity IDs from the given entity IDs

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs

        Returns
        -------
        List[List[ObjectId]]
        """
        reduced_entity_ids = self.reduce_entity_ids(entity_ids=entity_ids)
        entity_with_descendants_iterable = [
            self.entity_ancestor_descendant_mapper.entity_id_to_descendant_ids[entity_id]
            | {entity_id}
            for entity_id in reduced_entity_ids
        ]
        all_serving_entity_ids = set()
        for entity_id_combination in itertools.product(*entity_with_descendants_iterable):
            all_serving_entity_ids.add(
                tuple(self.reduce_entity_ids(entity_ids=list(entity_id_combination)))
            )

        output = [
            list(serving_entity)
            for serving_entity in sorted(all_serving_entity_ids, key=lambda e: (len(e), e))
        ]
        return output


class EntityRelationshipExtractorService:
    """
    Entity Relationship Extractor Service
    """

    def __init__(
        self,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    async def _extract_ancestors_or_descendants(
        self, entity_ids: Sequence[ObjectId], is_ancestor: bool
    ) -> List[ObjectId]:
        """
        Extract ancestors or descendants of the given entity IDs

        Parameters
        ----------
        entity_ids: Sequence[ObjectId]
            List of entity IDs
        is_ancestor: bool
            If True, extract ancestors. Otherwise, extract descendants

        Returns
        -------
        List[ObjectId]
        """
        output_entity_ids = set()
        entity_id_key = "_id" if is_ancestor else "ancestor_ids"
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={entity_id_key: {"$in": entity_ids}}
        ):
            if is_ancestor:
                output_entity_ids.update(entity.ancestor_ids)
            else:
                output_entity_ids.add(entity.id)
        return list(output_entity_ids)

    async def _extract_relationship_map(
        self, anchored_entity_ids: List[ObjectId], to_descendant: bool
    ) -> Dict[ObjectId, List[EntityRelationshipData]]:
        """
        Extract parent-child or child-parent entity relationship map from anchored entity IDs
        to its ancestors or descendants. If to_descendant is True, the parent-child relationship map
        will be extracted from the anchored entity IDs to its descendants. Otherwise, the child-parent
        relationship map will be extracted from anchored entity IDs to its ancestors.

        Parameters
        ----------
        anchored_entity_ids: List[ObjectId]
            List of anchored entity IDs
        to_descendant: bool
            If True, extract parent-child relationship map. Otherwise, extract child-parent relationship map

        Returns
        -------
        Dict[ObjectId, List[EntityRelationshipData]]
        """
        output_map: Dict[ObjectId, List[EntityRelationshipData]] = defaultdict(list)
        entity_id_key = "entity_id" if to_descendant else "related_entity_id"
        async for relationship in self.relationship_info_service.list_documents_iterator(
            query_filter={entity_id_key: {"$in": anchored_entity_ids}},
        ):
            if to_descendant:
                data_entity_id = relationship.entity_id
                anchored_entity_id = relationship.related_entity_id
            else:
                data_entity_id = relationship.related_entity_id
                anchored_entity_id = relationship.entity_id

            data = EntityRelationshipData(entity_id=data_entity_id, relationship_id=relationship.id)
            output_map[anchored_entity_id].append(data)
        return output_map

    @classmethod
    def _extract_entity_relationship_paths(
        cls,
        relationship_map: Dict[ObjectId, List[EntityRelationshipData]],
        path_map: Dict[ObjectId, List[ObjectId]],
        entity_id: ObjectId,
        relationship_ids: List[ObjectId],
    ) -> Dict[ObjectId, List[ObjectId]]:
        """
        Extract entity ID to list of relationship IDs mapping.

        Parameters
        ----------
        relationship_map: Dict[ObjectId, List[EntityRelationshipData]]
            Parent-child or child-parent relationship mapping
        path_map: Dict[ObjectId, List[ObjectId]]
            Mapping from entity ID to list of relationship IDs. If parent-child relationship mapping
            is provided, this method will recursively construct the mapping from child to parent
            relationship IDs. If child-parent relationship mapping is provided, this method will
            recursively construct the mapping from parent to child relationship IDs.
        entity_id: ObjectId
            Entity ID to be inserted into the path map
        relationship_ids: List[ObjectId]
            List of relationship IDs to be inserted into the path map

        Returns
        -------
        Dict[ObjectId, List[ObjectId]]
        """
        path_map[entity_id].extend(relationship_ids)
        for relation_data in relationship_map[entity_id]:
            path_map = cls._extract_entity_relationship_paths(
                relationship_map=relationship_map,
                path_map=path_map,
                entity_id=relation_data.entity_id,
                relationship_ids=path_map[entity_id] + [relation_data.relationship_id],
            )

        return path_map

    async def _get_entity_relationships(
        self, relationship_ids: List[ObjectId]
    ) -> List[EntityRelationshipInfo]:
        """
        Get entity relationships from relationship IDs

        Parameters
        ----------
        relationship_ids: List[ObjectId]
            List of relationship IDs

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        output = []
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={"_id": {"$in": relationship_ids}},
        ):
            output.append(EntityRelationshipInfo(**relationship_info.model_dump(by_alias=True)))
        return output

    async def get_entity_id_to_entity(
        self, entity_ids: List[ObjectId]
    ) -> Dict[ObjectId, EntityModel]:
        """
        Construct entity ID to entity dictionary mapping

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs

        Returns
        -------
        Dict[ObjectId, EntityModel]
            Dictionary mapping entity ID to entity model
        """
        entity_id_to_entity: Dict[ObjectId, EntityModel] = {}
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": entity_ids}}
        ):
            entity_id_to_entity[entity.id] = entity
        return entity_id_to_entity

    async def extract_relationship_from_primary_entity(
        self,
        entity_ids: List[ObjectId],
        primary_entity_ids: Optional[List[ObjectId]] = None,
    ) -> List[EntityRelationshipInfo]:
        """
        Extract relationships between entity ids and primary entity ids. This method is used to
        capture the entity relationships during feature creation.

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity ids
        primary_entity_ids: Optional[List[ObjectId]]
            List of primary entity ids

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        if primary_entity_ids is None:
            primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=entity_ids
            )

        ancestor_entity_ids = await self._extract_ancestors_or_descendants(
            entity_ids=primary_entity_ids, is_ancestor=True
        )
        child_to_parent_map = await self._extract_relationship_map(
            anchored_entity_ids=ancestor_entity_ids, to_descendant=False
        )
        path_map: Dict[ObjectId, List[ObjectId]] = defaultdict(list)
        for entity_id in primary_entity_ids:
            path_map = self._extract_entity_relationship_paths(
                relationship_map=child_to_parent_map,
                path_map=path_map,
                entity_id=entity_id,
                relationship_ids=[],
            )

        # get all required relationship ids
        relationship_ids = set()
        for entity_id in entity_ids:
            relationship_ids.update(path_map.get(entity_id, []))

        output = await self._get_entity_relationships(relationship_ids=list(relationship_ids))
        return output

    async def extract_primary_entity_descendant_relationship(
        self,
        primary_entity_ids: Sequence[ObjectId],
    ) -> List[EntityRelationshipInfo]:
        """
        Extract relationships between primary entity ids and their descendants. This method is used
        to capture the entity relationships during feature list creation.

        Parameters
        ----------
        primary_entity_ids: Sequence[ObjectId]
            List of primary entity ids

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        descendant_entity_ids = await self._extract_ancestors_or_descendants(
            entity_ids=primary_entity_ids, is_ancestor=False
        )
        parent_to_child_map = await self._extract_relationship_map(
            anchored_entity_ids=descendant_entity_ids, to_descendant=True
        )
        path_map: Dict[ObjectId, List[ObjectId]] = defaultdict(list)
        for entity_id in primary_entity_ids:
            path_map = self._extract_entity_relationship_paths(
                relationship_map=parent_to_child_map,
                path_map=path_map,
                entity_id=entity_id,
                relationship_ids=[],
            )

        # get all explored relationship ids
        relationship_ids = set()
        for rel_ids in path_map.values():
            relationship_ids.update(rel_ids)

        output = await self._get_entity_relationships(relationship_ids=list(relationship_ids))
        return output
