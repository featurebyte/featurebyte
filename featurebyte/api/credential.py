"""
Credential module
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.feature_store import FeatureStore
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.credential import CredentialModel, SessionCredential, StorageCredential
from featurebyte.schema.credential import CredentialUpdate


@typechecked
class Credential(CredentialModel, SavableApiObject):
    """
    Credential class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Credential"], proxy_class="featurebyte.Credential")

    # class variables
    _route = "/credential"
    _update_schema_class = CredentialUpdate
    _list_schema = CredentialModel
    _get_schema = CredentialModel
    _list_fields = ["name", "created_at", "updated_at", "credential_type"]

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

    @classmethod
    def create(
        cls,
        feature_store: FeatureStore,
        database_credential: Optional[SessionCredential],
        storage_credential: Optional[StorageCredential],
    ) -> Credential:
        """
        Create and return an instance of a credential.

        Parameters
        ----------
        feature_store: FeatureStore
            FeatureStore object to associate the credential with.
        database_credential: Optional[SessionCredential]
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
        credential = cls(
            name=feature_store.name,
            feature_store_id=feature_store.id,
            database_credential=database_credential,
            storage_credential=storage_credential,
        )
        credential.save()
        return credential

    def update_credential(
        self,
        database_credential: Optional[SessionCredential],
        storage_credential: Optional[StorageCredential],
    ) -> None:
        """
        Update credential details.

        Parameters
        ----------
        database_credential: Optional[SessionCredential]
            Session credential details.
        storage_credential: Optional[StorageCredential]
            Storage credential details.

        Examples
        --------
        >>> credential = fb.Credential.get_by_id("playground")  # doctest: +SKIP
        >>> credential.update_credential(
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
            allow_update_local=True,
        )
