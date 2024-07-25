"""
Credential module
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import DatabaseCredential, StorageCredential
from featurebyte.schema.credential import CredentialCreate, CredentialRead, CredentialUpdate


@typechecked
class Credential(DeletableApiObject, SavableApiObject):
    """
    Credential class is the data model used to represent your credentials that are persisted.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.Credential",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/credential"
    _update_schema_class: ClassVar[Any] = CredentialUpdate
    _list_schema: ClassVar[Any] = CredentialRead
    _get_schema: ClassVar[Any] = CredentialRead
    _list_fields: ClassVar[List[str]] = [
        "feature_store",
        "created_at",
        "updated_at",
        "database_credential",
        "storage_credential",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store"),
    ]

    # pydantic instance variable (internal use)
    internal_database_credential: Optional[DatabaseCredential] = Field(alias="database_credential")
    internal_storage_credential: Optional[StorageCredential] = Field(alias="storage_credential")

    # pydantic instance variable (public)
    feature_store_id: PydanticObjectId = Field(
        frozen=True,
        description="Id of the feature store that the credential is associated with.",
    )

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CredentialCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    @property
    def database_credential(self) -> Optional[DatabaseCredential]:
        """
        Get the database credential type.

        Returns
        -------
        Optional[DatabaseCredential]
            Database credential.
        """
        return self.cached_model.database_credential

    @property
    def storage_credential(self) -> Optional[StorageCredential]:
        """
        Get the storage credential type.

        Returns
        -------
        Optional[StorageCredential]
            Storage credential.
        """
        return self.cached_model.storage_credential

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
        ...     feature_store_name="playground",
        ...     database_credential=UsernamePasswordCredential(
        ...         username="username", password="password"
        ...     ),
        ...     storage_credential=S3StorageCredential(
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
