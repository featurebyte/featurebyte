"""
APIObjectMixin class
"""
from __future__ import annotations

from http import HTTPStatus

from featurebyte.config import Configurations
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel


class APIObject(FeatureByteBaseModel):
    """
    APIObjectMixin contains common methods used to interact with API routes
    """

    _route = ""

    @classmethod
    def _get_object_name(cls, class_name: str) -> str:
        object_name = "".join(
            "_" + char.lower() if char.isupper() else char for char in class_name
        ).lstrip("_")
        return object_name

    @classmethod
    def get(cls, name: str) -> APIObject:
        """
        Retrieve object dictionary from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        EventData
            EventData object of the given event data name

        Raises
        ------
        RecordRetrievalException
            When the event data not found
        """
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                object_dict = response_dict["data"][0]
                tabular_source = object_dict.get("tabular_source")
                if tabular_source:
                    feature_store_id, _ = tabular_source
                    feature_store_response = client.get(url=f"/feature_store/{feature_store_id}")
                    if feature_store_response.status_code == HTTPStatus.OK:
                        feature_store = ExtendedFeatureStoreModel(**feature_store_response.json())
                        return cls(**object_dict, feature_store=feature_store)
                    raise RecordRetrievalException(
                        response, f'FeatureStore (feature_store.id: "{feature_store_id}") not found!'
                    )
                return cls(**object_dict)

        class_name = cls.__name__
        object_name = cls._get_object_name(class_name)
        raise RecordRetrievalException(
            response, f'{class_name} ({object_name}.name: "{name}") not found!'
        )
