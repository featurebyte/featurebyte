"""
Test document model for stored credentials
"""

import copy
import json

import pytest

from featurebyte import SnowflakeDetails
from featurebyte.enum import SourceType
from featurebyte.models.credential import (
    AccessTokenCredential,
    AzureBlobStorageCredential,
    CredentialModel,
    DatabaseCredentialType,
    GCSStorageCredential,
    GoogleCredential,
    KerberosKeytabCredential,
    OAuthCredential,
    PrivateKeyCredential,
    S3StorageCredential,
    StorageCredentialType,
    UsernamePasswordCredential,
)
from featurebyte.models.feature_store import FeatureStoreModel


@pytest.fixture(
    name="storage_credential", params=[None] + list(StorageCredentialType.__members__.values())
)
def storage_credential_fixture(request):
    """
    Fixture for a StorageCredential object
    """
    if not request.param:
        return None
    if request.param == StorageCredentialType.S3:
        credential = S3StorageCredential(
            s3_access_key_id="access_key_id",
            s3_secret_access_key="secret_access_key",
        )
    elif request.param == StorageCredentialType.GCS:
        credential = GCSStorageCredential(
            service_account_info={
                "type": "service_account",
                "private_key": "private_key",
            }
        )
    elif request.param == StorageCredentialType.AZURE:
        credential = AzureBlobStorageCredential(
            account_name="account_name",
            account_key="account_key",
        )
    else:
        raise ValueError("Invalid storage credential type")
    return credential.json_dict()


@pytest.fixture(
    name="database_credential",
    params=[None] + list(DatabaseCredentialType.__members__.values()),
)
def database_credential_fixture(request):
    """
    Fixture for a DatabaseCredential object
    """
    if not request.param:
        return None
    if request.param == DatabaseCredentialType.ACCESS_TOKEN:
        credential = AccessTokenCredential(access_token="access_token")
    elif request.param == DatabaseCredentialType.OAUTH:
        credential = OAuthCredential(client_id="client_id", client_secret="client_secret")
    elif request.param == DatabaseCredentialType.USERNAME_PASSWORD:
        credential = UsernamePasswordCredential(username="test", password="password")
    elif request.param == DatabaseCredentialType.KERBEROS_KEYTAB:
        credential = KerberosKeytabCredential.from_file(
            keytab_filepath="tests/fixtures/hive.service.keytab",
            principal="principal@REALM",
        )
    elif request.param == DatabaseCredentialType.GOOGLE:
        credential = GoogleCredential(
            service_account_info={
                "type": "service_account",
                "private_key": "private_key",
            }
        )
    elif request.param == DatabaseCredentialType.PRIVATE_KEY:
        credential = PrivateKeyCredential.from_file(
            username="test",
            key_filepath="tests/fixtures/rsa_key_no_passphrase.p8",
        )
    else:
        raise ValueError("Invalid credential type")
    return credential.json_dict()


@pytest.fixture(name="feature_store")
def feature_store_fixture():
    """
    Fixture for a FeatureStoreModel object
    Returns
    -------
    FeatureStoreModel
        FeatureStoreModel object
    """
    return FeatureStoreModel(
        name="sf_featurestore",
        type=SourceType.SNOWFLAKE,
        details=SnowflakeDetails(
            account="account",
            warehouse="COMPUTE_WH",
            database_name="DATABASE",
            schema_name="PUBLIC",
            role_name="TESTING",
        ),
    )


def test_credentials_serialize_json(feature_store, database_credential, storage_credential):
    """
    Test serialization to json
    """
    credential = CredentialModel(
        name="SF Credentials",
        feature_store_id=feature_store.id,
        database_credential=database_credential,
        storage_credential=storage_credential,
    )
    credential_to_serialize = copy.deepcopy(credential)
    credential_to_serialize.encrypt_credentials()
    credential_json = credential_to_serialize.model_dump_json(by_alias=True)
    deserialized_credential = CredentialModel(**json.loads(credential_json))

    # check that the credential is encrypted
    if database_credential or storage_credential:
        assert deserialized_credential.json_dict() != credential.json_dict()

    # check that the credential is decrypted correctly
    deserialized_credential.decrypt_credentials()
    assert deserialized_credential.json_dict() == credential.json_dict()


def test_kerberos_keytab_credential_from_file():
    """
    Test KerberosKeytabCredential.from_file
    """
    credential = KerberosKeytabCredential.from_file(
        keytab_filepath="tests/fixtures/hive.service.keytab",
        principal="principal@REALM",
    )
    assert credential.principal == "principal@REALM"
    assert credential.type == DatabaseCredentialType.KERBEROS_KEYTAB

    with open("tests/fixtures/hive.service.keytab", "rb") as file_obj:
        assert credential.keytab == file_obj.read()


def test_gcs_storage_credential_service_account_info():
    """
    Test GCS storage credential accepts string service account info
    """

    credential = CredentialModel(**{
        "feature_store_id": "668f81a61f685fdecfce9ee9",
        "storage_credential": {
            "type": "GCS",
            "service_account_info": json.dumps({
                "type": "service_account",
                "private_key": "private_key",
            }),
        },
    })
    assert isinstance(credential.storage_credential, GCSStorageCredential)
    assert credential.storage_credential.service_account_info == {
        "type": "service_account",
        "private_key": "private_key",
    }


def test_private_key_credential_from_file():
    """
    Test PrivateKeyCredential.from_file
    """
    credential = PrivateKeyCredential.from_file(
        username="test",
        key_filepath="tests/fixtures/rsa_key.p8",
        passphrase="password",
    )
    assert credential.type == DatabaseCredentialType.PRIVATE_KEY

    # check that the passphrase is validated
    with pytest.raises(ValueError) as exc:
        PrivateKeyCredential.from_file(
            username="test",
            key_filepath="tests/fixtures/rsa_key.p8",
        )
    assert "Password was not given but private key is encrypted." in str(exc.value)

    with pytest.raises(ValueError) as exc:
        PrivateKeyCredential.from_file(
            username="test",
            key_filepath="tests/fixtures/rsa_key.p8",
            passphrase="wrong_password",
        )
    assert "Passphrase provided is incorrect for the private key." in str(exc.value)

    assert credential.username == "test"
    assert credential.passphrase == "password"
    with open("tests/fixtures/rsa_key.p8", "r") as file_obj:
        assert credential.private_key == file_obj.read()


def test_private_key_credential_from_file_without_passphrase():
    """
    Test PrivateKeyCredential.from_file without passphrase
    """
    credential = PrivateKeyCredential.from_file(
        username="test",
        key_filepath="tests/fixtures/rsa_key_no_passphrase.p8",
    )
    assert credential.type == DatabaseCredentialType.PRIVATE_KEY

    assert credential.username == "test"
    assert credential.passphrase is None
    with open("tests/fixtures/rsa_key_no_passphrase.p8", "r") as file_obj:
        assert credential.private_key == file_obj.read()


def test_private_key_credential():
    """
    Test PrivateKeyCredential copied from terminal (newlines converted to spaces)
    """
    credential = PrivateKeyCredential(
        username="test",
        private_key=(
            "-----BEGIN PRIVATE KEY----- "
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCq0H4LE8nuQXII "
            "fnILujJhTAoSLzVS148q2+aNm2a3k3S2nYrG57q4BSmeB3qPl49qbbTH5mrEaH+s "
            "Y0f2CV2wXfluM2Rr356j+LPBk1l9WObq23mN8YyI9as3JREInkLgNM7hkHH4Y/Pw "
            "dsXmnNZblrxfDk6DhIJk7fDEhVgkyqssEThBPTK+O2vlREwqjsV+0JTJgzSo6pXx "
            "rGUKdFs6p1L6Ll5B+4l6yZhM9zfOGDzRw0KN0Uiqqd3aV1VIqd6TEJXHlCxeY8x7 "
            "9UA8j/KqYXW2umBWCSyZS07/0sLCZRYEW/lcnHXIMltBDiZxy7Vs58nHEmm32Exp "
            "6Z5mh4wHAgMBAAECggEAIomDYrP+mUjsSKFgZ9SfCSM5yhF3g6eIEA9kX29zZlzP "
            "NXlHLQ5/p2OL1aMHee8YFVnXOq/xGINUhUORskKUieuGWmzTuif9yIOpvNMRPhHy "
            "prv3qOaVFsAtfTnmZsqxFOo4hk0RbjqvgQhS3V0Kznv83G9lGpx5TPT7QJEBkHaA "
            "7aRaM4HSkflWV4zFdGw+oWP93Qw6/B0RquamFQ2+mtHIrPvn3LPDiC7CeEu/6H3j "
            "1hQlIuuuBUPwZmekZL1pJF6BuhAndbjfWonMT9nXEygd0gqz566vyGWD1h/i7xcR "
            "bHeII3rGiaIMySAY3byQ6FYJKA9eiQIUjfCLuuJ6sQKBgQDpsoqi5+BMjyl4FKXW "
            "ymdqeR6OwqnLK7FMhpr/h+Nh/gan7nU17Zu55IbFR20krYTaF7YgZDBKfkHD3Z3F "
            "3AFPw7O8/RGPEqOalFHCquYRHjlN30Pb23ZV8XoF3UYmEyyfWTPgX/kwWZt3l9ON "
            "20VavTZQBVJyHBT+6Dk5lOZzOQKBgQC7HagztknKdvCQA3wIC0BQp6MVtphYE2kz "
            "7423bfmr5AqHQGpCsE193gvAb3Z7Iz7lYj7WfNc0Nema79aLCBYBArhcSVf8aD2E "
            "i3uPdyeS5hCu4Wb8Xe5TYRpnm8OOFFtm+JHlz1D9cL3n/DUg/EVt0HW7F8S/NUgN "
            "2WCci/W5PwKBgQC6r07CXhs27XJCI7RrBhtg2cqIXocG7yteJ3UwRdxlzmiAxCPL "
            "5bjt4dmrRKiykQ68rg5mh8Jv77YXgjTj9yDxGDO/+CWLtmcNOAisSpso94ztYToz "
            "Kni4pQNGJgJArjaKQNcJGYHVlu9ztMxh2NTpbJczi1zWHQrEqrvz/LevOQKBgGaZ "
            "/IFek1fRoFdXkctXYAzZ3zMozKB/BFDWKn9Kbn0yrhM73whyZAuAljEO7YjX6sUc "
            "+hfinJ6kcVPj72CNLoOfWjhAf16ISjNDyJ0CWVDTlpJORopbdzOBK1lkr/ZYc0Yj "
            "Rt0csOxHxdpPEVLlAa0VgXj1r4ypSrlNWQx+Ml9BAoGAG2oFDc0mcNCj8tPmcvOG "
            "9qxUTH/bWFXZ8guR9sPHssb4l+nySBePceXJYaT6osDXvMOP840dZ62hgZZSXA5I "
            "ClhgFWWIwwnOWtjx2nSqdtyUDnsIKtEj7qamNwAcqNIKnI5ai/MBgwuOyKtS9myX "
            "3ydYyQWYhdq42BXQDMndUqM= "
            "-----END PRIVATE KEY-----"
        ),
        passphrase=None,
    )
    assert credential.type == DatabaseCredentialType.PRIVATE_KEY

    assert credential.username == "test"
    assert credential.passphrase is None
