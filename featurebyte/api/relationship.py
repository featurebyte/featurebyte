"""
Relationships API object
"""
from typing import Literal, Optional

from http import HTTPStatus

import pandas as pd
from typeguard import typechecked

from featurebyte import Configurations
from featurebyte.api.api_object import ApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.relationship import RelationshipInfo, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoUpdate


class Relationship(RelationshipInfo, ApiObject):
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

        client = Configurations().get_client()
        response = client.patch(url=f"/relationship_info/{self.id}", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
