"""
Unit tests for the UserDefinedFunction class.
"""
import pandas as pd
import pytest

from featurebyte.api.catalog import Catalog
from featurebyte.api.user_defined_function import UDF, UserDefinedFunction
from featurebyte.exception import DocumentCreationError, RecordDeletionException
from featurebyte.models.user_defined_function import FunctionParameter


@pytest.fixture(name="catalog")
def catalog_fixture(snowflake_feature_store):
    """Catalog fixture"""
    return Catalog.create(name="test_catalog", feature_store_name=snowflake_feature_store.name)


@pytest.fixture(name="cos_udf")
def cos_udf_fixture(catalog):
    """Cos UDF fixture"""
    func_param = FunctionParameter(
        name="x",
        dtype="FLOAT",
        default_value=None,
        test_value=None,
        has_default_value=False,
        has_test_value=False,
    )
    udf = UserDefinedFunction.create(
        name="cos_func",
        function_name="cos",
        function_parameters=[func_param],
        output_dtype="FLOAT",
        is_global=True,
    )
    yield udf


@pytest.fixture(name="power_udf")
def pow_udf_fixture(catalog):
    """Power UDF fixture"""
    common_func_param_kwargs = {
        "default_value": None,
        "test_value": None,
        "has_default_value": False,
        "has_test_value": False,
    }
    udf = UserDefinedFunction.create(
        name="power_func",
        function_name="power",
        function_parameters=[
            FunctionParameter(name="base", dtype="FLOAT", **common_func_param_kwargs),
            FunctionParameter(name="exp", dtype="FLOAT", **common_func_param_kwargs),
        ],
        output_dtype="FLOAT",
        is_global=True,
    )
    yield udf


@pytest.fixture(name="date_sub_udf")
def date_sub_udf_fixture(catalog):
    """Date sub UDF fixture"""
    common_func_param_kwargs = {
        "default_value": None,
        "test_value": None,
        "has_default_value": False,
        "has_test_value": False,
    }
    udf = UserDefinedFunction.create(
        name="date_sub_func",
        function_name="date_sub",
        function_parameters=[
            FunctionParameter(name="start_date", dtype="TIMESTAMP_TZ", **common_func_param_kwargs),
            FunctionParameter(name="num_days", dtype="INT", **common_func_param_kwargs),
        ],
        output_dtype="TIMESTAMP_TZ",
        is_global=False,
    )
    yield udf


def test_create_user_defined_function__default_catalog():
    """Test create_user_defined_function (default catalog)"""
    with pytest.raises(DocumentCreationError) as exc:
        UserDefinedFunction.create(
            name="udf_func",
            function_name="cos",
            function_parameters=[],
            output_dtype="FLOAT",
            is_global=True,
        )

    expected_error = (
        'Catalog "default" does not have a default feature store. '
        "Please activate a catalog with a default feature store first before creating a user-defined function."
    )
    assert expected_error in str(exc.value)


def test_create_user_defined_function(catalog, cos_udf):
    """Test create_user_defined_function"""
    assert cos_udf.name == "cos_func"
    assert cos_udf.function_name == "cos"
    assert cos_udf.output_dtype == "FLOAT"
    assert cos_udf.catalog_id is None
    assert cos_udf.feature_store_id == catalog.default_feature_store_ids[0]
    assert cos_udf.function_parameters == [
        FunctionParameter(
            name="x",
            dtype="FLOAT",
            default_value=None,
            test_value=None,
            has_default_value=False,
            has_test_value=False,
        )
    ]

    # check the UDF class has the function now
    assert cos_udf.name in dir(UDF)

    # delete the UDF
    cos_udf.delete()

    # check the UDF class does not have the function anymore
    assert cos_udf.name not in dir(UDF)


def test_list(cos_udf, power_udf, date_sub_udf):
    """Test list user-defined functions"""
    udf_list = UserDefinedFunction.list()
    pd.testing.assert_frame_equal(
        udf_list,
        pd.DataFrame(
            {
                "id": [date_sub_udf.id, power_udf.id, cos_udf.id],
                "signature": [date_sub_udf.signature, power_udf.signature, cos_udf.signature],
                "function_name": [
                    date_sub_udf.function_name,
                    power_udf.function_name,
                    cos_udf.function_name,
                ],
                "feature_store_name": "sf_featurestore",
                "is_global": [date_sub_udf.is_global, power_udf.is_global, cos_udf.is_global],
            }
        ),
    )


def test_create_feature_with_user_defined_function(cos_udf, float_feature):
    """Test create a feature with user-defined function"""
    cos_feat = UDF.cos_func(float_feature)
    cos_feat.name = "cos_feat"
    cos_feat.save()

    # check feature model has expected properties
    assert cos_feat.dtype == "FLOAT"
    assert cos_feat.cached_model.user_defined_function_ids == [cos_udf.id]

    # attempt to delete the UDF should fail
    with pytest.raises(RecordDeletionException) as exc:
        cos_udf.delete()

    expected_error = "User defined function used by saved feature(s): ['cos_feat']"
    assert expected_error in str(exc.value)

    # delete the feature first then delete the UDF should succeed
    cos_feat.delete()
    cos_udf.delete()


def test_create_complex_feature_with_user_defined_function(
    power_udf, date_sub_udf, float_feature, non_time_based_feature
):
    """Test create a complex feature with user-defined function"""
    # user defined function with multiple feature inputs
    power_feat1 = UDF.power_func(float_feature, non_time_based_feature)
    power_feat1.name = "power_feat1"
    power_feat1.save()

    # user defined function with a feature and a constant
    power_feat2 = UDF.power_func(float_feature, 2)
    power_feat2.name = "power_feat2"
    power_feat2.save()

    # date_sub function with a feature and a constant
    date_sub_feat = UDF.date_sub_func(
        start_date=pd.Timestamp("2020-01-01"), num_days=non_time_based_feature.astype(int)
    )
    date_sub_feat.name = "date_sub_feat"
    date_sub_feat.save()

    # check features have expected properties
    assert power_feat1.dtype == "FLOAT"
    assert power_feat2.dtype == "FLOAT"
    assert date_sub_feat.dtype == "TIMESTAMP_TZ"
    assert power_feat1.cached_model.user_defined_function_ids == [power_udf.id]
    assert power_feat2.cached_model.user_defined_function_ids == [power_udf.id]
    assert date_sub_feat.cached_model.user_defined_function_ids == [date_sub_udf.id]

    # attempt to delete the UDF should fail
    with pytest.raises(RecordDeletionException) as exc:
        power_udf.delete()
    expected_error = (
        "User defined function used by saved feature(s): ['power_feat2', 'power_feat1']"
    )
    assert expected_error in str(exc.value)

    with pytest.raises(RecordDeletionException) as exc:
        date_sub_udf.delete()
    expected_error = "User defined function used by saved feature(s): ['date_sub_feat']"
    assert expected_error in str(exc.value)

    # delete the feature first then delete the UDF should succeed (for power_udf)
    power_feat1.delete()
    power_feat2.delete()
    power_udf.delete()

    # delete the feature first then delete the UDF should succeed (for date_sub)
    date_sub_feat.delete()
    date_sub_udf.delete()


def test_create_feature_with_complex_view_operation(
    power_udf, cos_udf, snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """Test create a feature with complex view operation"""
    view = snowflake_event_view_with_entity
    view["float_square"] = UDF.power_func(view.col_float, 2)
    view["cos_float_square"] = UDF.cos_func(x=view.float_square)
    feat_group = view.groupby("cust_id").aggregate_over(
        value_column="cos_float_square",
        method="sum",
        windows=["1d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_cos_float_square"],
    )
    feat = feat_group["sum_cos_float_square"]
    feat.name = "sum_cos_float_square"
    feat.save()

    # check feature model has expected properties
    assert sorted(feat.cached_model.user_defined_function_ids) == sorted([power_udf.id, cos_udf.id])

    # attempt to delete the UDF should fail
    expected_error = "User defined function used by saved feature(s): ['sum_cos_float_square']"
    with pytest.raises(RecordDeletionException) as exc:
        power_udf.delete()
    assert expected_error in str(exc.value)

    with pytest.raises(RecordDeletionException) as exc:
        cos_udf.delete()
    assert expected_error in str(exc.value)

    # delete the feature first then delete the UDF should succeed
    feat.delete()
    power_udf.delete()
    cos_udf.delete()
