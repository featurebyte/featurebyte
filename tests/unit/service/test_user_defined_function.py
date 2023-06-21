"""
Test UserDefinedFunctionService
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.schema.user_defined_function import UserDefinedFunctionCreate
from featurebyte.service.user_defined_function import UserDefinedFunctionService


@pytest.fixture(name="user_defined_function_service")
def user_defined_function_service_fixture(app_container):
    """User defined function service fixture"""
    return app_container.user_defined_function_service


@pytest.fixture(name="user_defined_function_service_with_different_catalog")
def user_defined_function_service_with_different_catalog_fixture(app_container):
    """User defined function service with different catalog fixture"""
    return UserDefinedFunctionService(
        user=app_container.user_defined_function_service.user,
        persistent=app_container.user_defined_function_service.persistent,
        catalog_id=ObjectId(),
    )


@pytest.fixture(name="user_defined_function_dict")
def user_defined_function_dict_fixture():
    """User defined function dict fixture"""
    return {
        "name": "method_name",
        "function_name": "sql_function_name",
        "function_parameters": [
            {
                "name": "param1",
                "dtype": "INT",
                "default_value": None,
                "test_value": None,
                "has_default_value": False,
                "has_test_value": False,
            },
        ],
        "catalog_id": None,
        "output_dtype": "FLOAT",
    }


@pytest_asyncio.fixture(name="global_user_defined_function_doc")
async def global_user_defined_function_doc_fixture(
    user_defined_function_service, user_defined_function_dict
):
    """Global user defined function doc fixture"""
    payload = user_defined_function_dict.copy()
    payload["name"] = "global_method_name"
    payload["catalog_id"] = None
    doc = await user_defined_function_service.create_document(
        data=UserDefinedFunctionCreate(**payload)
    )
    assert doc.catalog_id is None
    return doc


@pytest_asyncio.fixture(name="user_defined_function_doc")
async def user_defined_function_doc_fixture(
    user_defined_function_service, user_defined_function_dict
):
    """User defined function doc fixture"""
    payload = user_defined_function_dict.copy()
    payload["catalog_id"] = DEFAULT_CATALOG_ID
    doc = await user_defined_function_service.create_document(
        data=UserDefinedFunctionCreate(**payload)
    )
    assert doc.catalog_id == DEFAULT_CATALOG_ID
    return doc


@pytest.mark.asyncio
async def test_user_defined_function_service__creation(
    user_defined_function_service,
    user_defined_function_service_with_different_catalog,
    user_defined_function_doc,
    user_defined_function_dict,
    global_user_defined_function_doc,
):
    """Test UserDefinedFunctionService (creation)"""
    # create a user defined function with the same name in the same catalog
    user_defined_function_dict["catalog_id"] = DEFAULT_CATALOG_ID
    with pytest.raises(DocumentConflictError) as exc:
        await user_defined_function_service.create_document(
            data=UserDefinedFunctionCreate(**user_defined_function_dict)
        )
    expected_error_message = (
        'User defined function with name "method_name" already exists in catalog '
        f"(catalog_id: {DEFAULT_CATALOG_ID})."
    )
    assert expected_error_message in str(exc.value)

    # create a user defined function conflicting with the global user defined function
    with pytest.raises(DocumentConflictError) as exc:
        await user_defined_function_service.create_document(
            data=UserDefinedFunctionCreate(**global_user_defined_function_doc.dict(by_alias=True))
        )
    expected_error_message = (
        'Global user defined function with name "global_method_name" already exists.'
    )
    assert expected_error_message in str(exc.value)

    # create a user defined function with the same name in a different catalog
    another_catalog_id = user_defined_function_service_with_different_catalog.catalog_id
    user_defined_function_dict["catalog_id"] = str(another_catalog_id)
    doc_with_same_name = await user_defined_function_service_with_different_catalog.create_document(
        data=UserDefinedFunctionCreate(**user_defined_function_dict)
    )
    assert doc_with_same_name.name == user_defined_function_doc.name


@pytest.mark.asyncio
async def test_user_defined_function_service__retrieval(
    user_defined_function_service,
    user_defined_function_service_with_different_catalog,
    user_defined_function_doc,
    global_user_defined_function_doc,
):
    """Test UserDefinedFunctionService (retrieve)"""
    # check that the global user defined function can be retrieved
    retrieved_doc = await user_defined_function_service.get_document(
        document_id=global_user_defined_function_doc.id
    )
    assert retrieved_doc == global_user_defined_function_doc

    retrieved_doc = await user_defined_function_service_with_different_catalog.get_document(
        document_id=global_user_defined_function_doc.id
    )
    assert retrieved_doc == global_user_defined_function_doc

    # check retrieving the non-global user defined function
    retrieved_doc = await user_defined_function_service.get_document(
        document_id=user_defined_function_doc.id
    )
    assert retrieved_doc == user_defined_function_doc

    with pytest.raises(DocumentNotFoundError):
        await user_defined_function_service_with_different_catalog.get_document(
            document_id=user_defined_function_doc.id
        )
