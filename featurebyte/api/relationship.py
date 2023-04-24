"""
Relationships API object
"""
from typing import Literal, Optional

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfo, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoUpdate


class Relationship(ApiObject):
    """
    The relationships class allows users to see explore what types of relationships exist between the various
    entities they have.

    We currently support child-parent relationships that are a many-to-one relationship between entities.
    These relationships are used to:

    - recommend features from a parent for a use case
    - recommend features recipes such as similarity between the child and the parent
    - serve feature list

    Relationships are automatically established when an entity is a primary key (or natural key) of a table and other
    entities are tagged in this table. The entity that is the primary key (or natural key) is automatically set as
    the child of the other entities. The table that maps the relationship is tagged in the relationship to facilitate
    the serving of the features.
    """

    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Relationship",
    )

    # class variables
    _route = "/relationship_info"
    _update_schema_class = RelationshipInfoUpdate
    _get_schema = RelationshipInfo
    _list_schema = RelationshipInfo
    _list_fields = [
        "relationship_type",
        "entity",
        "related_entity",
        "relation_table",
        "relation_table_type",
        "enabled",
        "created_at",
        "updated_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_id", Entity, "entity"),
        ForeignKeyMapping("related_entity_id", Entity, "related_entity"),
        ForeignKeyMapping("relation_table_id", TableApiObject, "relation_table"),
        ForeignKeyMapping("relation_table_id", TableApiObject, "relation_table_type", "type"),
    ]

    # pydantic instance variable (internal use)
    internal_enabled: bool = Field(alias="enabled")
    internal_updated_by: PydanticObjectId = Field(alias="updated_by")

    @property
    def enabled(self) -> bool:
        """
        Whether the relationship has been enabled

        Returns
        -------
        bool
            Whether the relationship has been enabled
        """
        try:
            return self.cached_model.enabled
        except RecordRetrievalException:
            return self.internal_enabled

    @property
    def updated_by(self) -> PydanticObjectId:
        """
        Who the relationship was updated by

        Returns
        -------
        PydanticObjectId
            User ID of the user who updated the relationship
        """
        try:
            return self.cached_model.updated_by
        except RecordRetrievalException:
            return self.internal_updated_by

    @classmethod
    @typechecked
    def list(
        cls, include_id: Optional[bool] = True, relationship_type: Optional[Literal[tuple(RelationshipType)]] = None  # type: ignore[misc]
    ) -> pd.DataFrame:
        """
        List all relationships that exist in your FeatureByte instance, or filtered by relationship type.

        This provides a dataframe with:

        - the relationship id
        - primary entity
        - related entity
        - table source
        - enabled (whether the relationship is enabled)
        - creation timestamp
        - update timestamp
        - comments

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include the id in the dataframe
        relationship_type: Optional[Literal[tuple[RelationshipType]]]
            The type of relationship to list

        Returns
        -------
        pd.DataFrame
            A dataframe containing the relationships

        Examples
        --------
        List all relationships

        >>> fb.Relationship.list()[[
        ...     "relationship_type",
        ...     "entity",
        ...     "related_entity",
        ... ]]
          relationship_type           entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate


        List all child-parent relationships

        >>> fb.Relationship.list(relationship_type="child_parent")[[
        ...     "relationship_type",
        ...     "entity",
        ...     "related_entity",
        ... ]]
          relationship_type           entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate
        """
        list_responses = super().list(include_id=include_id)
        if relationship_type:
            list_responses = list_responses[
                list_responses["relationship_type"] == relationship_type
            ]
        return list_responses

    def enable(self) -> None:
        """
        Enables a Relationship object.

        A Relationship object of parent-child is automatically created when the primary key (or natural key in the
        context of a SCD table) identifies one entity. This entity is the child entity. Other entities that are
        referenced in the table are identified as the parent entities.

        Examples
        --------
        Enable a relationship

        >>> import featurebyte as fb  # doctest: +SKIP
        >>> relationship = fb.Relationship.get_by_id(<relationship_id>)  # doctest: +SKIP
        >>> relationship.enable()  # doctest: +SKIP
        """
        self.update({"enabled": True}, allow_update_local=True, add_internal_prefix=True)

    def disable(self) -> None:
        """
        Disables a Relationship object.

        A Relationship object of parent-child is automatically created when the primary key (or natural key in the
        context of a SCD table) identifies one entity. This entity is the child entity. Other entities that are
        referenced in the table are identified as the parent entities.

        Examples
        --------
        Disable a relationship

        >>> import featurebyte as fb  # doctest: +SKIP
        >>> relationship = fb.Relationship.get_by_id(<relationship_id>)  # doctest: +SKIP
        >>> relationship.disable()  # doctest: +SKIP
        """
        self.update({"enabled": False}, allow_update_local=True, add_internal_prefix=True)
