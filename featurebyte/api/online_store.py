"""
OnlineStore class
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from bson import ObjectId
from pandas import DataFrame

from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.online_store import OnlineStoreDetails, OnlineStoreModel
from featurebyte.schema.online_store import OnlineStoreCreate


class OnlineStore(OnlineStoreModel, SavableApiObject, DeletableApiObject):
    """
    OnlineStore class to represent an online store in FeatureByte.
    This class is used to manage an online store in FeatureByte.

    The purpose of an Online Store is to host the most recent feature values for low latency serving.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.OnlineStore")
    _route: ClassVar[str] = "/online_store"
    _list_schema: ClassVar[Any] = OnlineStoreModel
    _get_schema: ClassVar[Any] = OnlineStoreModel
    _list_fields: ClassVar[List[str]] = ["name", "details", "created_at"]

    def _get_create_payload(self) -> dict[str, Any]:
        data = OnlineStoreCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of the online store represented by the
        OnlineStore object. The dictionary contains the following keys:

        - `name`: The name of the online store.
        - `created_at`: The timestamp indicating when the online store owas created.
        - `updated_at`: The timestamp indicating when the OnlineStore object was last updated.
        - `details`: The configuration details of the online store.
        - `description`: The description of the online store.

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
        >>> online_store = fb.OnlineStore.get(<online_store_name>)  # doctest: +SKIP
        >>> online_store.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def create(
        cls,
        name: str,
        details: OnlineStoreDetails,
    ) -> OnlineStore:
        """
        Creates and saves a Online Store object to enable FeatureByte to serve online features.

        To create an online store, you need to specify the connection details and credentials to use.

        Only one online store can be created for a specific configuration details.

        Parameters
        ----------
        name: str
            Name of the online store.
        details: OnlineStoreDetails
            Configuration details to use for the online store.

        Returns
        -------
        OnlineStore

        Examples
        --------
        >>> online_store = fb.OnlineStore.create(  # doctest: +SKIP
        ...     name="online store",
        ...     details=fb.MySQLOnlineStoreDetails(
        ...         host="mysql_host",
        ...         database="mysql_database",
        ...         user="mysql_user",
        ...         password="mysql_password",
        ...         port=3306,
        ...     ),
        ... )
        """
        # Construct object, and save to persistent layer.
        online_store = OnlineStore(
            name=name,
            details=details,
        )
        online_store.save()
        return online_store

    @classmethod
    def get_or_create(
        cls,
        name: str,
        details: OnlineStoreDetails,
    ) -> OnlineStore:
        """
        Create and return an instance of an online store. If an online store with the same name already exists,
        return that instead.

        Note that only one online store can be created for a specific configuration details.

        Parameters
        ----------
        name: str
            Name of the online store.
        details: OnlineStoreDetails
            Configuration details to use for the online store.

        Returns
        -------
        OnlineStore

        Examples
        --------
        Create a online store housed in a Snowflake database

        >>> online_store = fb.OnlineStore.get_or_create(
        ...     name="online store", details=fb.RedisOnlineStoreDetails()
        ... )
        >>> OnlineStore.list()[["name"]]
                         name
        0        online store
        1  mysql_online_store

        See Also
        --------
        - [OnlineStore.create](/reference/featurebyte.api.online_store.OnlineStore.create/): Create OnlineStore
        """
        try:
            return OnlineStore.get(name=name)
        except RecordRetrievalException:
            return OnlineStore.create(
                name=name,
                details=details,
            )

    @classmethod
    def get(cls, name: str) -> OnlineStore:
        """
        Gets a OnlineStore object by its name.

        Parameters
        ----------
        name: str
            Name of the online store to retrieve.

        Returns
        -------
        OnlineStore
            OnlineStore object.

        Examples
        --------
        Get a OnlineStore object that is already saved.

        >>> online_store = fb.OnlineStore.get("online_store_name")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> OnlineStore:
        """
        Returns a OnlineStore object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            OnlineStore unique identifier (ID) to retrieve.

        Returns
        -------
        OnlineStore
            OnlineStore object.

        Examples
        --------
        Get a OnlineStore object by its Object ID.

        >>> fb.OnlineStore.get_by_id("646f6c190ed28a5271fb02b9")  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        Returns a DataFrame that lists the online stores by their names, types and creation dates.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        List all online stores.

        >>> online_stores = fb.OnlineStore.list()
        """
        return super().list(include_id=include_id)

    def delete(self) -> None:
        """
        Deletes the online store from the persistent data store. The online store can only be deleted if it is not
        being referenced by any catalog.

        Examples
        --------
        >>> online_store = fb.OnlineStore.get_by_id("646f6c190ed28a5271fb02b9")  # doctest: +SKIP
        >>> online_store.delete()  # doctest: +SKIP
        """
        self._delete()
