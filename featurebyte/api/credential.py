"""
Credential module
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from http import HTTPStatus

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ForeignKeyMapping, SavableApiObject
from featurebyte.api.feature_store import FeatureStore
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordDeletionException, RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import (
    CredentialModel,
    DatabaseCredential,
    DatabaseCredentialType,
    StorageCredential,
    StorageCredentialType,
)
from featurebyte.schema.credential import CredentialRead, CredentialUpdate


@typechecked
class Credential(SavableApiObject):
    """
    Credential class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Credential"], proxy_class="featurebyte.Credential")

    # class variables
    _route = "/credential"
    _update_schema_class = CredentialUpdate
    _list_schema = CredentialRead
    _get_schema = CredentialModel
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
    internal_feature_store_id: PydanticObjectId = Field(alias="feature_store_id")
    internal_database_credential: Optional[DatabaseCredential] = Field(alias="database_credential")
    internal_storage_credential: Optional[StorageCredential] = Field(alias="storage_credential")

    # pydantic instance variable (public)
    saved: bool = Field(
        default=False,
        allow_mutation=False,
        exclude=True,
        description="Flag to indicate whether the Credential object is saved in the FeatureByte catalog.",
    )

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CredentialModel(**self.json_dict())
        return data.json_dict()

    @property
    def feature_store_id(self) -> ObjectId:
        """
        Get the feature store id that the credential is associated with.

        Returns
        -------
        ObjectId
            Feature store id.
        """
        try:
            return self.cached_model.feature_store_id
        except RecordRetrievalException:
            return self.internal_feature_store_id

    @property
    def database_credential_type(self) -> Optional[DatabaseCredentialType]:
        """
        Get the database credential type.

        Returns
        -------
        Optional[DatabaseCredentialType]
            Database credential type.
        """
        database_credential_type = self.info().get("database_credential_type")
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
        storage_credential_type = self.info().get("storage_credential_type")
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

    @classmethod
    def delete(cls, id: ObjectId) -> None:  # pylint: disable=redefined-builtin,invalid-name
        """
        Delete a credential. Note that associated feature store will no longer be able to access the data warehouse
        until a new credential is created. Please use with caution.

        Parameters
        ----------
        id : ObjectId
            Credential id

        Raises
        ------
        RecordDeletionException
            If the credential cannot be deleted

        Examples
        --------
        Delete a credential
        >>> credential = fb.Credential.get("playground")
        >>> fb.Credential.delete(credential.id)  # doctest: +SKIP

        See Also
        --------
        - [Credential.update_credentials](/reference/featurebyte.api.credential.Credential.update_credentials/)
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.config import Configurations

        client = Configurations().get_client()
        response = client.delete(url=f"{cls._route}/{id}")
        if response.status_code != HTTPStatus.NO_CONTENT:
            raise RecordDeletionException(response, "Failed to delete the specified credential.")

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
