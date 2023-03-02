"""
Relationships API object
"""
from typing import Literal, Optional

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfo, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoUpdate


class Relationship(ApiObject):
    """
    The relationships class allows users to see explore what types of relationships exist between the various
    entities they have.
    """

    # class variables
    _route = "/relationship_info"
    _update_schema_class = RelationshipInfoUpdate
    _get_schema = RelationshipInfo
    _list_schema = RelationshipInfo
    _list_fields = [
        "relationship_type",
        "primary_entity_id",
        "related_entity_id",
        "primary_data_source_id",
        "is_enabled",
        "updated_by",
        "created_at",
        "updated_at",
    ]

    # pydantic instance variable (internal use)
    internal_is_enabled: bool = Field(alias="is_enabled")
    internal_updated_by: PydanticObjectId = Field(alias="updated_by")

    @property
    def is_enabled(self) -> bool:
        """
        Whether the relationship has been updated

        Returns
        -------
        bool
            Whether the relationship has been updated
        """
        try:
            return self.cached_model.is_enabled
        except RecordRetrievalException:
            return self.internal_is_enabled

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
        List a dataframe of the relationships. This provides a dataframe with:
        - the relationship id
        - primary entity
        - relate dentity
        - data source
        - enabled (whether the relationship is enabled)
        - creation timestamp
        - update timestamp
        - updated by
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
        """
        list_responses = super().list(include_id=include_id)
        if relationship_type:
            list_responses = list_responses[
                list_responses["relationship_type"] == relationship_type
            ]
        return list_responses

    @typechecked
    def enable(self, enable: bool) -> None:
        """
        Enable

        Parameters
        ----------
        enable: bool
            Whether to enable or disable the relationship

        Raises
        ------
        RecordRetrievalException
            raised if the record cannot be retrieved
        """

        payload = RelationshipInfoUpdate(
            is_enabled=enable,
        )
        self.update(payload.json_dict(), allow_update_local=True, add_internal_prefix=True)
