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


def test_resource_details__change_view():
    """
    Test extracting resource details on get_change_view.

    We test this class specifically because the parameters were not getting their types extracted properly. This was
    due to some unknown issue in type_hints. Due to lack of time, a workaround is used to just rely on the annotation
    of the parameter, instead of trying to infer the type. The downside is that this type is just a string, and
    doesn't have any contextual information.
    """
    details = get_resource_details("featurebyte.api.scd_table.SCDTable::get_change_view")
    assert details == ResourceDetails(
        name="get_change_view",
        realname="get_change_view",
        path="featurebyte.api.scd_table.SCDTable",
        proxy_path="featurebyte.SCDTable",
        type="method",
        base_classes=None,
        method_type=None,
        short_description="Gets a ChangeView from a Slowly Changing Dimension (SCD) table. The view offers a "
        "method to examine alterations",
        long_description="that occur in a specific attribute within the natural key of the SCD table.\n\n"
        "To create the ChangeView, you need to provide the name of the SCD column for which you want to track "
        "changes\nthrough the track_changes_column parameter.\n\nOptionally,\n\n- you can define the default "
        "Feature Job Setting for the View. Default is once a day, at the time of the\ncreation of the view.\n"
        "- you can provide a `prefix` parameter to control how the views columns are named.\n\nThe resulting view "
        "has 5 columns:\n\n- the natural key of the SCDView\n- past_<name_of_column_tracked>: value of the column "
        "before the change\n- new_<name_of_column_tracked>: value of the column after the change\n- past_valid_"
        "from_timestamp (equal to the effective timestamp of the SCD before the change)\n- new_valid_from_"
        "timestamp (equal to the effective timestamp of the SCD after the change)\n\nThe ChangeView can be used "
        "to create Aggregates of Changes Over a Window features, similar to Aggregates Over\na Window features "
        "created from an Event View.",
        parameters=[
            ParameterDetails(
                name="track_changes_column",
                type="str",
                default=None,
                description="Name of the column to track changes for.",
            ),
            ParameterDetails(
                name="default_feature_job_setting",
                type="Optional[FeatureJobSetting]",
                default="None",
                description="Default feature job setting to set with the FeatureJobSetting constructor. If "
                "not provided, the default\nfeature job setting is daily, aligning with the view's creation time.",
            ),
            ParameterDetails(
                name="prefixes",
                type="Optional[Tuple[Optional[str], Optional[str]]]",
                default="None",
                description="Optional prefixes where each element indicates the prefix to add to the new column "
                "names for the name of\nthe column that we want to track. The first prefix will be used for the "
                "old, and the second for the new.\nIf not provided, the column names will be prefixed with the "
                'default values of "past_", and "new_". At\nleast one of the values must not be None. If two '
                "values are provided, they must be different.",
            ),
            ParameterDetails(
                name="view_mode",
                type="Literal[ViewMode.AUTO, ViewMode.MANUAL]",
                default='"auto"',
                description="View mode to use. When auto, the view will be constructed with cleaning operations.",
            ),
            ParameterDetails(
                name="drop_column_names",
                type="Optional[List[str]]",
                default="None",
                description="List of column names to drop (manual mode only).",
            ),
            ParameterDetails(
                name="column_cleaning_operations",
                type="Optional[List[ColumnCleaningOperation]]",
                default="None",
                description="List of cleaning operations to apply per column in manual mode only. Each element "
                "in the list indicates the\ncleaning operations for a specific column. The association between "
                "this column and the cleaning operations\nis established via the ColumnCleaningOperation "
                "constructor.",
            ),
        ],
        returns=ParameterDetails(
            name=None,
            type="ChangeView",
            default=None,
            description="ChangeView object constructed from the SCD source table.",
        ),
        raises=[],
        examples=[
            "Get a ChangeView.\n",
            '\n```pycon\n>>> scd_table = fb.Table.get("GROCERYCUSTOMER")\n>>> change_view = scd_table.'
            'get_change_view(\n...     track_changes_column="CurrentRecord",\n...     prefixes=("previous_",'
            ' "next_"),\n... )\n```\n',
        ],
        see_also=None,
        enum_values=[],
        should_skip_params_in_class_docs=True,
        should_skip_signature_in_class_docs=True,
        should_hide_keyword_only_params_in_class_docs=False,
    )
