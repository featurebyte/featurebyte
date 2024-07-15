"""
SavableApiObject class
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Optional

from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, get_api_object_cache_key
from featurebyte.api.api_object_util import delete_api_object_by_id
from featurebyte.common.utils import is_server_mode
from featurebyte.config import Configurations
from featurebyte.enum import ConflictResolution
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
)


class SavableApiObject(ApiObject):
    """
    ApiObject contains common methods used to interact with API routes
    """

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Construct payload used for post route

        Returns
        -------
        dict[str, Any]
        """
        return self.json_dict(exclude_none=True)

    def _check_object_not_been_saved(self, conflict_resolution: ConflictResolution) -> None:
        if self.saved and conflict_resolution == "raise":
            raise ObjectHasBeenSavedError(
                f'{type(self).__name__} (id: "{self.id}") has been saved before.'
            )

    @typechecked
    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
    ) -> None:
        """
        Save an object to the persistent data store.

        A conflict could be triggered when the object being saved has violated a uniqueness check at the persistent
        data store. For example, the same object ID could have been used by another record that is already stored.

        In these scenarios, we can either raise an error or retrieve the object with the same name, depending on the
        conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" will raise an error when we encounter a conflict error.
            "retrieve" will handle the conflict error by retrieving the object with the same name.
        _id: Optional[ObjectId]
            The object ID to be used when saving the object. If not provided, a new object ID will be generated.

        Raises
        ------
        DuplicatedRecordException
            When a record with the same key exists at the persistent data store.
        RecordCreationException
            When we fail to save the new object (general failure).

        Examples
        --------
        Note that the examples below are not exhaustive.

        Save a new Entity object.

        >>> entity = fb.Entity(
        ...     name="grocerycustomer_example", serving_names=["GROCERYCUSTOMERGUID"]
        ... )  # doctest: +SKIP
        >>> entity.save()  # doctest: +SKIP
        None

        Calling save again returns an error.

        >>> entity = fb.Entity(
        ...     name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"]
        ... )  # doctest: +SKIP
        >>> entity.save()  # doctest: +SKIP
        >>> entity.save()  # doctest: +SKIP
        Entity (id: <entity.id>) has been saved before.
        """
        if not is_server_mode():
            # skip the check when running the SDK in the server mode (avoid making an API call)
            self._check_object_not_been_saved(conflict_resolution=conflict_resolution)

        client = Configurations().get_client()
        payload = self._get_create_payload()
        if _id is not None:
            payload["_id"] = str(_id)
        response = client.post(url=self._route, json=payload)
        if is_server_mode():
            # this is used only when running the SDK in the batch feature creation task (server mode)
            return

        retrieve_object = False
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                if conflict_resolution == "retrieve":
                    retrieve_object = True
                else:
                    raise DuplicatedRecordException(response=response)
            if not retrieve_object:
                raise RecordCreationException(response=response)

        if retrieve_object:
            assert self.name is not None
            object_dict = self._get_object_dict_by_name(name=self.name)
        else:
            object_dict = response.json()

        self._update_cache(object_dict)  # update api object cache store
        type(self).__init__(
            self,
            **object_dict,
            **self._get_init_params_from_object(),
        )


class DeletableApiObject(ApiObject):
    """
    DeleteMixin contains common methods used to delete an object
    """

    def _delete(self) -> None:
        delete_api_object_by_id(route=self._route, id_value=self.id)

        # update api object cache store to remove the deleted object
        cache_key = get_api_object_cache_key(self)
        self._cache.pop(cache_key, None)
