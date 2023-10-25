"""
Entity Relationship Combiner Service
"""
from typing import Dict, List, Set, Tuple

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.exception import EntityRelationshipConflictError
from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.models.relationship import RelationshipType


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

    def __init__(self) -> None:
        self.id_to_ancestors: Dict[ObjectId, Set[AncestorData]] = defaultdict(set)

    def reset(self) -> None:
        """
        Reset the validator
        """
        self.id_to_ancestors = defaultdict(set)

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
            self.id_to_ancestors[entity_id].add(AncestorData(parent_entity_id, (feature_name,)))
            for ancestor in self.id_to_ancestors[parent_entity_id]:
                self.id_to_ancestors[entity_id].add(
                    AncestorData(
                        ancestor_id=ancestor.ancestor_id,
                        feature_names=ancestor.feature_names + (feature_name,),
                    )
                )

    def _validate_relationship(
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
            parent_ancestors = self.id_to_ancestors[relationship.related_entity_id]
            for ancestor in parent_ancestors:
                if ancestor.ancestor_id == relationship.entity_id:
                    feature_names = sorted(set(ancestor.feature_names))
                    raise EntityRelationshipConflictError(
                        f"Entity {relationship.entity_id} is an ancestor of "
                        f"{relationship.related_entity_id} (based on features: {feature_names}) "
                        f"but feature '{feature_name}' has a child-parent relationship between them."
                    )

        self._update_ancestor_mapping(relationship=relationship, feature_name=feature_name)

    def validate(
        self,
        relationships: List[EntityRelationshipInfo],
        feature_name: str,
    ) -> None:
        """
        Combine entity relationships

        Parameters
        ----------
        relationships: List[EntityRelationshipInfo]
            Relationships of the feature
        feature_name: str
            Feature name
        """
        for relationship in relationships:
            self._validate_relationship(relationship=relationship, feature_name=feature_name)
