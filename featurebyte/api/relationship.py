"""
Relationships API object
"""
from typing import Literal, Optional

import json
from http import HTTPStatus

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
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

    Relationships are automatically established when an entity is a primary key (or natural key) of a data and other
    entities are tagged in this data. The entity that is the primary key (or natural key) is automatically set as
    the child of the other entities. The data that maps the relationship is tagged in the relationship to facilitate
    the serving of the features.
    """

    __fbautodoc__ = FBAutoDoc(
        section=["Relationship"],
        proxy_class="featurebyte.Relationship",
    )

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

    @staticmethod
    def _convert_relationship_info_model_df_to_named_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a dataframe of relationship info models to a dataframe with named columns

        Parameters
        ----------
        df: pd.DataFrame
            A dataframe of relationship info models

        Returns
        -------
        pd.DataFrame
            A dataframe with named columns
        """
        # Get IDs of relationship info's
        relationship_info_ids = []
        for _, row in df.iterrows():
            relationship_info_ids.append(row["id"])
        # Get named information for each relationship info using the info route.
        relationship_infos = []
        client = Configurations().get_client()
        for relationship_info_id in relationship_info_ids:
            response = client.get(url=f"/relationship_info/{relationship_info_id}/info")
            if response.status_code != HTTPStatus.OK:
                raise RecordRetrievalException(response, "Failed to retrieve object info.")
            relationship_infos.append(response.json())

        # Convert info to dataframe
        new_df = pd.read_json(json.dumps(relationship_infos))
        # We need to convert the id to string to merge as the original type is ObjectId and won't merge with the str
        # id type in the new dataframe.
        df["id"] = df["id"].astype("string")
        joined_result = df.merge(new_df, on="id", how="left", suffixes=("_left", "_right"))
        # Filter for the columns we want
        filtered_result = joined_result[
            [
                "id",
                "relationship_type_left",
                "primary_entity_name",
                "related_entity_name",
                "data_source_name",
                "data_type",
                "is_enabled",
                "created_at_right",
                "updated_at_right",
                "updated_by_right",
            ]
        ]
        # Rename columns appropriately.
        return filtered_result.rename(
            columns={
                "relationship_type_left": "relationship_type",
                "created_at_right": "created_at",
                "updated_at_right": "updated_at",
                "updated_by_right": "updated_by",
            }
        )

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

        Examples
        --------
        List all relationships

        >>> import featurebyte as fb
        >>> fb.Relationship.list()  # doctest: +SKIP


        List all child-parent relationships

        >>> import featurebyte as fb
        >>> fb.Relationship.list(relationship_type="child_parent")  # doctest: +SKIP
        """
        list_responses = super().list(include_id=include_id)
        if relationship_type:
            list_responses = list_responses[
                list_responses["relationship_type"] == relationship_type
            ]
        if list_responses.shape[0] == 0:
            return list_responses

        return cls._convert_relationship_info_model_df_to_named_df(list_responses)

    @typechecked
    def enable(self, enable: bool) -> None:
        """
        Update the relationship to enable or disable it.

        Parameters
        ----------
        enable: bool
            Whether to enable or disable the relationship

        Examples
        --------
        Enable a relationship

        >>> import featurebyte as fb
        >>> relationship = fb.Relationship.get_by_id(<relationship_id>)  # doctest: +SKIP
        >>> relationship.enable(True)


        Disable a relationship

        >>> import featurebyte as fb
        >>> relationship = fb.Relationship.get_by_id(<relationship_id>)  # doctest: +SKIP
        >>> relationship.enable(False)
        """

        payload = RelationshipInfoUpdate(
            is_enabled=enable,
        )
        self.update(payload.json_dict(), allow_update_local=True, add_internal_prefix=True)
