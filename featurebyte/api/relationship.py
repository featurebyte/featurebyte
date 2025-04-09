"""
Relationships API object
"""

from typing import Any, ClassVar, Dict, List, Optional, Union

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Relationship")
    _route: ClassVar[str] = "/relationship_info"
    _update_schema_class: ClassVar[Any] = RelationshipInfoUpdate
    _get_schema: ClassVar[Any] = RelationshipInfoModel
    _list_schema: ClassVar[Any] = RelationshipInfoModel
    _list_fields: ClassVar[List[str]] = [
        "relationship_type",
        "entity",
        "related_entity",
        "relation_table",
        "relation_table_type",
        "enabled",
        "created_at",
        "updated_at",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("entity_id", Entity, "entity"),
        ForeignKeyMapping("related_entity_id", Entity, "related_entity"),
        ForeignKeyMapping("relation_table_id", TableApiObject, "relation_table"),
        ForeignKeyMapping("relation_table_id", TableApiObject, "relation_table_type", "type"),
    ]

    # pydantic instance variable (internal use)
    internal_enabled: bool = Field(alias="enabled")
    internal_updated_by: Optional[PydanticObjectId] = Field(alias="updated_by")

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
    def updated_by(self) -> Optional[PydanticObjectId]:
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

    @property
    def catalog_id(self) -> PydanticObjectId:
        """
        Returns the unique identifier (ID) of the Catalog that is associated with the Relationship object.

        Returns
        -------
        PydanticObjectId
            Catalog ID of the relationship.

        See Also
        --------
        - [Catalog](/reference/featurebyte.api.catalog.Catalog)
        """
        return self.cached_model.catalog_id

    @classmethod
    @typechecked
    def list(
        cls,
        include_id: Optional[bool] = True,
        relationship_type: Optional[Union[RelationshipType, str]] = None,
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
        relationship_type: Optional[Union[RelationshipType, str]]
            The type of relationship to list

        Returns
        -------
        pd.DataFrame
            A dataframe containing the relationships

        Examples
        --------
        List all relationships

        >>> fb.Relationship.list()[
        ...     [
        ...         "relationship_type",
        ...         "entity",
        ...         "related_entity",
        ...     ]
        ... ]
          relationship_type           entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate


        List all child-parent relationships

        >>> fb.Relationship.list(relationship_type="child_parent")[
        ...     [
        ...         "relationship_type",
        ...         "entity",
        ...         "related_entity",
        ...     ]
        ... ]
          relationship_type           entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate
        """
        type_value = RelationshipType(relationship_type).value if relationship_type else None
        list_responses = super().list(include_id=include_id)
        if relationship_type:
            list_responses = list_responses[list_responses["relationship_type"] == type_value]
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

    def update_relationship_type(self, relationship_type: RelationshipType) -> None:
        """
        Updates the relationship type of a Relationship object.

        Parameters
        ----------
        relationship_type: RelationshipType
            The new relationship type to set for the Relationship object.

        Examples
        --------
        >>> import featurebyte as fb  # doctest: +SKIP
        >>> relationship = fb.Relationship.get_by_id(<relationship_id>)  # doctest: +SKIP
        >>> relationship.update_relationship_type(RelationshipType.ONE_TO_ONE)  # doctest: +SKIP
        """
        self.update(
            {"relationship_type": relationship_type},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of the relationship represented by the
        Relationship object. The dictionary contains the following keys:

        - `relationship_type`: The relationship type
        - `entity_id`: The main entity of the relationship
        - `related_entity_id`: The entity that the relationship is related to
        - `relation_table_id`: The table that maps the relationship
        - `enabled`: Whether the relationship is enabled
        - `updated_by`: Who the relationship was updated by

        Parameters
        ----------
        verbose: bool
            The parameter "verbose" in the current state of the code does not have any impact on the output.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> relationship = fb.Relationship.get(<relationship_name>)  # doctest: +SKIP
        >>> relationship.info()  # doctest: +SKIP
        """
        return super().info(verbose)
