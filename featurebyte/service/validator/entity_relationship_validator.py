"""
Entity Relationship Combiner Service
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from bson import ObjectId

from featurebyte.exception import EntityRelationshipConflictError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.entity import EntityService


@dataclass(frozen=True)
class AncestorData:
    """
    Ancestor data
    """

    ancestor_id: ObjectId
    feature_names: Tuple[str, ...]


class FeatureListEntityRelationshipValidator:
    """
    Feature list entity relationship validator is responsible for validating entity relationships
    of features within a given feature list.
    """

    def __init__(self, entity_service: EntityService) -> None:
        self.entity_service = entity_service
        self._id_to_ancestors: Dict[ObjectId, Set[AncestorData]] = defaultdict(set)

    def _update_ancestor_mapping(
        self, relationship: EntityRelationshipInfo, feature_name: str
    ) -> None:
        """
        Add ancestor IDs to the ID to ancestor IDs mapping

        Parameters
        ----------
        relationship: EntityRelationshipInfo
            Relationship to add
        feature_name: str
            Feature name of the relationship
        """
        if relationship.relationship_type == RelationshipType.CHILD_PARENT:
            entity_id = relationship.entity_id
            parent_entity_id = relationship.related_entity_id
            self._id_to_ancestors[entity_id].add(AncestorData(parent_entity_id, (feature_name,)))
            for ancestor in self._id_to_ancestors[parent_entity_id]:
                self._id_to_ancestors[entity_id].add(
                    AncestorData(
                        ancestor_id=ancestor.ancestor_id,
                        feature_names=ancestor.feature_names + (feature_name,),
                    )
                )

    async def _validate_relationship(
        self, relationship: EntityRelationshipInfo, feature_name: str
    ) -> None:
        """
        Validate relationship by checking if it conflicts with existing relationships

        Parameters
        ----------
        relationship: EntityRelationshipInfo
            Relationship to add
        feature_name: str
            Feature name of the relationship

        Raises
        ------
        EntityRelationshipConflictError
            If the relationship conflicts with an existing relationship
        """
        if relationship.relationship_type == RelationshipType.CHILD_PARENT:
            # check if the relationship conflicts with existing relationships
            parent_ancestors = self._id_to_ancestors[relationship.related_entity_id]
            for ancestor in parent_ancestors:
                if ancestor.ancestor_id == relationship.entity_id:
                    ancestor_feature_names = sorted(set(ancestor.feature_names))
                    entity = await self.entity_service.get_document(
                        document_id=relationship.entity_id
                    )
                    related_entity = await self.entity_service.get_document(
                        document_id=relationship.related_entity_id
                    )
                    raise EntityRelationshipConflictError(
                        f"Entity '{entity.name}' is an ancestor of "
                        f"'{related_entity.name}' (based on features: {ancestor_feature_names}) "
                        f"but '{entity.name}' is a child of '{related_entity.name}' based on '{feature_name}'. "
                        f"Consider excluding '{feature_name}' from the Feature List to fix the error."
                    )

        self._update_ancestor_mapping(relationship=relationship, feature_name=feature_name)

    async def _validate(
        self,
        relationships: List[EntityRelationshipInfo],
        feature_name: str,
    ) -> None:
        """
        Helper to validate entity relationships of a feature

        Parameters
        ----------
        relationships: List[EntityRelationshipInfo]
            Relationships of the feature
        feature_name: str
            Feature name
        """
        for relationship in relationships:
            await self._validate_relationship(relationship=relationship, feature_name=feature_name)

    async def validate(self, features: List[FeatureModel]) -> None:
        """
        Validate entity relationships of features

        Parameters
        ----------
        features: List[FeatureModel]
            List of Feature model
        """
        self._id_to_ancestors = defaultdict(set)
        for feature in features:
            if feature.relationships_info:
                assert feature.name is not None
                await self._validate(
                    relationships=feature.relationships_info,
                    feature_name=feature.name,
                )
