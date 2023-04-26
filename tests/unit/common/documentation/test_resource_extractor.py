"""
Test resource extractor.
"""
from pydantic import Field

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.doc_types import (
    ExceptionDetails,
    ParameterDetails,
    ResourceDetails,
)
from featurebyte.common.documentation.resource_extractor import get_resource_details
from featurebyte.enum import StrEnum


class TestDocumentation:
    """
    Class documentation string.

    Let's add more lines to this documentation to make this more realistic, and also to make sure
    that it is parsed correctly.

    See Also
    --------
    Some text that normally references other resources.

    Examples
    --------
    Some example code.
    """

    pydantic_int_field: int = Field(description="pydantic int field description")

    def __init__(self, constructor_param: str):
        self.constructor_param = constructor_param

    @property
    def constructor_param_property(self) -> str:
        """
        Property documentation string.

        Returns
        -------
        str
            Property string
        """
        return self.constructor_param

    def instance_method(self, arg1: int) -> str:
        """
        Method documentation string.

        Add more description for extra length, and extra parsing!!

        Parameters
        ----------
        arg1: int
            Some description

        Returns
        -------
        str
            Some description

        Raises
        ------
        Exception
            Some description

        Examples
        --------
        Some example code.
        """
        if arg1 == 1:
            raise Exception("arg is 1")
        elif self.constructor_param == "error":
            raise Exception("constructor param is error")
        return "test method"


class TestDocumentationEnum(StrEnum):
    """
    Test documentation for enum classes.
    """

    TEST = "test", "test field 1"
    TEST2 = "test2", "test field 2"
    NO_DESCRIPTION = "no desc"


class DocClassWithFbAutoDocParams:
    """
    Doc class with FBAutoDoc proxy path, and skip params.
    """

    __fbautodoc__ = FBAutoDoc(
        proxy_class="autodoc_proxy",
        skip_params_and_signature_in_class_docs=True,
        hide_keyword_only_params_in_class_docs=True,
    )


def test_get_resource_details__enum_class():
    """
    Test get_resource_details for enum classes.
    """
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentationEnum"
    )
    expected_resource_details = ResourceDetails(
        name="TestDocumentationEnum",
        realname="TestDocumentationEnum",
        path="tests.unit.common.documentation.test_resource_extractor",
        proxy_path=None,
        type="class",
        base_classes=["StrEnum"],
        method_type=None,
        short_description="Test documentation for enum classes.",
        long_description=None,
        parameters=[],
        returns=ParameterDetails(name=None, type=None, default=None, description=None),
        raises=[],
        examples=[],
        see_also=None,
        enum_values=[
            ParameterDetails(
                name="TEST",
                type=None,
                default="None",
                description="test field 1",
            ),
            ParameterDetails(
                name="TEST2",
                type=None,
                default="None",
                description="test field 2",
            ),
            ParameterDetails(
                name="NO DESC",
                type=None,
                default="None",
                description="Test documentation for enum classes.",
            ),
        ],
        should_skip_params_in_class_docs=False,
        should_skip_signature_in_class_docs=False,
        should_hide_keyword_only_params_in_class_docs=False,
    )
    assert resource_details == expected_resource_details


def test_get_resource_details__class():
    """
    Test get_resource_details for classes.
    """
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentation"
    )
    expected_resource_details = ResourceDetails(
        name="TestDocumentation",
        realname="TestDocumentation",
        path="tests.unit.common.documentation.test_resource_extractor",
        proxy_path=None,
        type="class",
        base_classes=["object"],
        method_type=None,
        short_description="Class documentation string.",
        long_description="Let's add more lines to this documentation to make this more realistic, and also to make "
        "sure\nthat it is parsed correctly.",
        parameters=[
            ParameterDetails(name="constructor_param", type="str", default=None, description=None)
        ],
        returns=ParameterDetails(name=None, type=None, default=None, description=None),
        raises=[],
        examples=["Some example code."],
        see_also="Some text that normally references other resources.",
        enum_values=[],
        should_skip_params_in_class_docs=False,
        should_skip_signature_in_class_docs=False,
        should_hide_keyword_only_params_in_class_docs=False,
    )
    assert resource_details == expected_resource_details


def test_get_resource_details__method():
    """
    Test get_resource_details for method.
    """
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentation::instance_method"
    )
    expected_resource_details = ResourceDetails(
        name="instance_method",
        realname="instance_method",
        path="tests.unit.common.documentation.test_resource_extractor.TestDocumentation",
        proxy_path=None,
        type="method",
        base_classes=None,
        method_type=None,
        short_description="Method documentation string.",
        long_description="Add more description for extra length, and extra parsing!!",
        parameters=[
            ParameterDetails(name="arg1", type="int", default=None, description="Some description")
        ],
        returns=ParameterDetails(
            name=None, type="str", default=None, description="Some description"
        ),
        raises=[ExceptionDetails(type="Exception", description="Some description")],
        examples=["Some example code."],
        see_also=None,
        enum_values=[],
        should_skip_params_in_class_docs=False,
        should_skip_signature_in_class_docs=False,
        should_hide_keyword_only_params_in_class_docs=False,
    )
    assert resource_details == expected_resource_details


def test_get_resource_details__pydantic_field():
    """
    Test get_resource_details for pydantic fields.
    """
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentation::pydantic_int_field"
    )
    expected_resource_details = ResourceDetails(
        name="pydantic_int_field",
        realname="pydantic_int_field",
        path="tests.unit.common.documentation.test_resource_extractor.TestDocumentation",
        proxy_path=None,
        type="property",
        base_classes=None,
        method_type=None,
        short_description="Captures extra information about a field.",  # TODO: this is wrong
        long_description=None,
        parameters=[],
        returns=ParameterDetails(name=None, type="FieldInfo", default=None, description=None),
        raises=[],
        examples=[],
        see_also=None,
        enum_values=[],
        should_skip_params_in_class_docs=False,
        should_skip_signature_in_class_docs=False,
        should_hide_keyword_only_params_in_class_docs=False,
    )
    assert resource_details == expected_resource_details


def test_get_resource_details__property():
    """
    Test get_resource_details for properties.
    """
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentation::constructor_param_property"
    )
    expected_resource_details = ResourceDetails(
        name="constructor_param_property",
        realname="constructor_param_property",
        path="tests.unit.common.documentation.test_resource_extractor.TestDocumentation",
        proxy_path=None,
        type="property",
        base_classes=None,
        method_type=None,
        short_description="Property documentation string.",
        long_description=None,
        parameters=[],
        returns=ParameterDetails(
            name=None, type="str", default=None, description="Property string"
        ),
        raises=[],
        examples=[],
        see_also=None,
        enum_values=[],
        should_skip_params_in_class_docs=False,
        should_skip_signature_in_class_docs=False,
        should_hide_keyword_only_params_in_class_docs=False,
    )
    assert resource_details == expected_resource_details


def test_get_resource_details__proxy_paths():
    """
    Test proxy paths
    """
    # Can specify via # separator
    details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.TestDocumentation::constructor_param_property#proxy"
    )
    assert details.proxy_path == "proxy"

    # Or infer via FBAutoDoc override
    resource_details = get_resource_details(
        "tests.unit.common.documentation.test_resource_extractor.DocClassWithFbAutoDocParams"
    )
    assert resource_details.proxy_path == "autodoc_proxy"
    assert resource_details.should_skip_params_in_class_docs
    assert resource_details.should_skip_signature_in_class_docs
    assert resource_details.should_hide_keyword_only_params_in_class_docs
