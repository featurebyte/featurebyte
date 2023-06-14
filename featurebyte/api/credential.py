"""
Credential module
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import (
    DatabaseCredential,
    DatabaseCredentialType,
    StorageCredential,
    StorageCredentialType,
)
from featurebyte.schema.credential import CredentialCreate, CredentialRead, CredentialUpdate


@typechecked
class Credential(DeletableApiObject, SavableApiObject):
    """
    Credential class is the data model used to represent your credentials that are persisted.

    Examples
    --------
    >>> credential = Credential(  # doctest: +SKIP
    ...   name=feature_store.name,
    ...   feature_store_id=feature_store.id,
    ...   database_credential=database_credential,
    ...   storage_credential=storage_credential,
    ... )
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Credential",
        skip_params_and_signature_in_class_docs=True,
    )

    # class variables
    _route = "/credential"
    _update_schema_class = CredentialUpdate
    _list_schema = CredentialRead
    _get_schema = CredentialRead
    _list_fields = [
        "feature_store",
        "created_at",
        "updated_at",
        "database_credential_type",
        "storage_credential_type",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store"),
    ]

    # pydantic instance variable (internal use)
    internal_database_credential: Optional[DatabaseCredential] = Field(alias="database_credential")
    internal_storage_credential: Optional[StorageCredential] = Field(alias="storage_credential")

    # pydantic instance variable (public)
    feature_store_id: PydanticObjectId = Field(
        allow_mutation=False,
        description="Id of the feature store that the credential is associated with.",
    )

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CredentialCreate(**self.dict(by_alias=True))
        return data.json_dict()

    @property
    def database_credential_type(self) -> Optional[DatabaseCredentialType]:
        """
        Get the database credential type.

        Returns
        -------
        Optional[DatabaseCredentialType]
            Database credential type.
        """
        database_credential_type = self.cached_model.database_credential_type
        if database_credential_type:
            return DatabaseCredentialType(database_credential_type)
        return None

    @property
    def storage_credential_type(self) -> Optional[StorageCredentialType]:
        """
        Get the storage credential type.

        Returns
        -------
        Optional[StorageCredentialType]
            Storage credential type.
        """
        storage_credential_type = self.cached_model.storage_credential_type
        if storage_credential_type:
            return StorageCredentialType(storage_credential_type)
        return None

    @classmethod
    def create(
        cls,
        feature_store_name: str,
        database_credential: Optional[DatabaseCredential] = None,
        storage_credential: Optional[StorageCredential] = None,
    ) -> Credential:
        """
        Create and return an instance of a credential.

        Parameters
        ----------
        feature_store_name: str
            Name of feature store to associate the credential with.
        database_credential: Optional[DatabaseCredential]
            Session credential details.
        storage_credential: Optional[StorageCredential]
            Storage credential details.

        Returns
        -------
        Credential

        Examples
        --------
        Create a new credential.

        >>> credential = fb.Credential.create(  # doctest: +SKIP
        ...     feature_store=fb.FeatureStore.get("playground"),
        ...     database_credential=UsernamePasswordCredential(
        ...         username="username",
        ...         password="password"
        ...     ),
        ...     storage_credentials=S3StorageCredential(
        ...         s3_access_key_id="access_key_id",
        ...         s3_secret_access_key="s3_secret_access_key",
        ...     ),
        ... )
        """
        feature_store = FeatureStore.get(feature_store_name)
        credential = Credential(
            name=feature_store.name,
            feature_store_id=feature_store.id,
            database_credential=database_credential,
            storage_credential=storage_credential,
        )
        credential.save()
        return credential

    def delete(self) -> None:
        """
        Delete a credential. Note that associated feature store will no longer be able to access the data warehouse
        until a new credential is created. Please use with caution.

        Examples
        --------
        Delete a credential.
        >>> credential = fb.Credential.get("playground")
        >>> credential.delete()  # doctest: +SKIP

        See Also
        --------
        - [Credential.update_credentials](/reference/featurebyte.api.credential.Credential.update_credentials/)
        """
        self._delete()

    def update_credentials(
        self,
        database_credential: Optional[DatabaseCredential] = None,
        storage_credential: Optional[StorageCredential] = None,
    ) -> None:
        """
        Update credential details.

        Parameters
        ----------
        database_credential: Optional[DatabaseCredential]
            Session credential details.
        storage_credential: Optional[StorageCredential]
            Storage credential details.

        Examples
        --------
        >>> credential = fb.Credential.get_by_id("playground")  # doctest: +SKIP
        >>> credential.update_credentials(  # doctest: +SKIP
        ...     storage_credential=S3StorageCredential(
        ...         s3_access_key_id="access_key_id",
        ...         s3_secret_access_key="s3_secret_access_key",
        ...     ),
        ... )
        """
        self.update(
            update_payload=CredentialUpdate(
                database_credential=database_credential,
                storage_credential=storage_credential,
            ).json_dict(),
            allow_update_local=False,
        )
