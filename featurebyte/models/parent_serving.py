"""
Models related to serving parent features
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import root_validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class EntityLookupInfo(FeatureByteBaseModel):
    """
    Information about an entity such as keys, serving names, that are relevant in the context of an
    EntityLookupStep

    entity_id: PydanticObjectId
        Entity id
    key: str
        Column name in the table that is tagged as the entity
    serving_name: str
        Serving name of the entity. Alternatively, this can be thought of as the input or output
        column name of the parent entity lookup operation.
    """

    entity_id: PydanticObjectId
    key: str
    serving_name: str


class EntityLookupStep(FeatureByteBaseModel):
    """
    EntityLookupStep contains all information required to perform a join between two related
    entities for the purpose of serving parent features

    id: PydanticObjectId
        Identifier of the EntityRelationshipInfo corresponding to this lookup step
    table: ProxyTableModel
        The table encoding the relationship between the two entities
    parent: EntityLookupInfo
        Information about the parent entity
    child: EntityLookupInfo
        Information about the child entity
    """

    id: PydanticObjectId
    table: ProxyTableModel
    parent: EntityLookupInfo
    child: EntityLookupInfo


class EntityLookupStepCreator(FeatureByteBaseModel):
    """
    Helper class containing concrete instances of EntityModel and TableModel to help with creating
    EntityLookupStep
    """

    entity_relationships_info: List[EntityRelationshipInfo]
    entities_by_id: Dict[PydanticObjectId, EntityModel]
    tables_by_id: Dict[PydanticObjectId, ProxyTableModel]
    default_entity_lookup_steps: Dict[PydanticObjectId, EntityLookupStep]

    @root_validator(pre=True)
    @classmethod
    def _generate_default_entity_lookup_steps(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        entity_relationships_info = values["entity_relationships_info"]
        entities_by_id = values["entities_by_id"]
        tables_by_id = values["tables_by_id"]
        default_entity_lookup_steps = {}

        for info in entity_relationships_info:
            relation_table = tables_by_id[info.relation_table_id]
            parent_entity = entities_by_id[info.related_entity_id]
            child_entity = entities_by_id[info.entity_id]

            child_column_name = None
            parent_column_name = None
            for column_info in relation_table.columns_info:
                if column_info.entity_id == child_entity.id:
                    child_column_name = column_info.name
                elif column_info.entity_id == parent_entity.id:
                    parent_column_name = column_info.name
            assert child_column_name is not None
            assert parent_column_name is not None

            default_entity_lookup_steps[info.id] = EntityLookupStep(
                id=info.id,
                table=relation_table.dict(by_alias=True),
                parent=EntityLookupInfo(
                    key=parent_column_name,
                    serving_name=parent_entity.serving_names[0],
                    entity_id=parent_entity.id,
                ),
                child=EntityLookupInfo(
                    key=child_column_name,
                    serving_name=child_entity.serving_names[0],
                    entity_id=child_entity.id,
                ),
            )

        values["default_entity_lookup_steps"] = default_entity_lookup_steps
        return values

    def get_entity_lookup_step(
        self,
        relationship_info_id: PydanticObjectId,
        child_serving_name_override: Optional[str] = None,
        parent_serving_name_override: Optional[str] = None,
    ) -> EntityLookupStep:
        """
        Get a EntityLookupStep object given the id of the relationship info and optional serving
        name overrides

        Parameters
        ----------
        relationship_info_id: PydanticObjectId
            Id of the EntityRelationshipInfo
        child_serving_name_override: Optional[str]
            Override child entity's serving name. This is the input column name for the parent
            entity lookup step.
        parent_serving_name_override: Optional[str]
            Override parent entity's serving name. This is the output column name for the parent
            entity lookup step.

        Returns
        -------
        EntityLookupStep
        """
        assert relationship_info_id in self.default_entity_lookup_steps
        entity_lookup_step = self.default_entity_lookup_steps[relationship_info_id]
        if child_serving_name_override is not None or parent_serving_name_override is not None:
            entity_lookup_step = entity_lookup_step.copy()
            if child_serving_name_override is not None:
                entity_lookup_step.child.serving_name = child_serving_name_override
            if parent_serving_name_override is not None:
                entity_lookup_step.parent.serving_name = parent_serving_name_override
        return entity_lookup_step


class ParentServingPreparation(FeatureByteBaseModel):
    """
    Operations required to serve parent features from children entities

    join_steps: List[JoinStep]
        List of JoinSteps identified based on the provided entities
    feature_store_details: FeatureStoreDetails
        Feature store information
    """

    join_steps: List[EntityLookupStep]
    feature_store_details: FeatureStoreDetails
