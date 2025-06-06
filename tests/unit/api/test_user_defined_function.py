"""
Unit tests for the UserDefinedFunction class.
"""

import re

import pandas as pd
import pytest

from featurebyte.api.catalog import Catalog
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.request_column import RequestColumn
from featurebyte.api.user_defined_function import UDF, UserDefinedFunction
from featurebyte.common import activate_catalog
from featurebyte.exception import (
    RecordCreationException,
    RecordDeletionException,
    RecordUpdateException,
)
from featurebyte.models.user_defined_function import FunctionParameter
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="catalog")
def catalog_fixture(snowflake_feature_store):
    """Catalog fixture"""
    yield Catalog.create(name="test_catalog", feature_store_name=snowflake_feature_store.name)


@pytest.fixture(name="cos_udf")
def cos_udf_fixture(catalog):
    """Cos UDF fixture"""
    func_param = FunctionParameter(name="x", dtype="FLOAT")
    udf = UserDefinedFunction.create(
        name="cos_func",
        sql_function_name="cos",
        function_parameters=[func_param],
        output_dtype="FLOAT",
        is_global=True,
    )
    yield udf


@pytest.fixture(name="local_cos_udf")
def local_cos_udf_fixture(catalog):
    """Local cos UDF fixture"""
    func_param = FunctionParameter(name="x", dtype="FLOAT")
    udf = UserDefinedFunction.create(
        name="cos_func",
        sql_function_name="cos_v2",
        function_parameters=[func_param],
        output_dtype="FLOAT",
        is_global=False,
    )
    yield udf


@pytest.fixture(name="power_udf")
def pow_udf_fixture(catalog):
    """Power UDF fixture"""
    udf = UserDefinedFunction.create(
        name="power_func",
        sql_function_name="power",
        function_parameters=[
            FunctionParameter(name="base", dtype="FLOAT"),
            FunctionParameter(name="exp", dtype="FLOAT"),
        ],
        output_dtype="FLOAT",
        is_global=True,
    )
    yield udf


@pytest.fixture(name="date_sub_udf")
def date_sub_udf_fixture(catalog):
    """Date sub UDF fixture"""
    udf = UserDefinedFunction.create(
        name="date_sub_func",
        sql_function_name="date_sub",
        function_parameters=[
            FunctionParameter(name="start_date", dtype="TIMESTAMP_TZ"),
            FunctionParameter(name="num_days", dtype="INT"),
        ],
        output_dtype="TIMESTAMP_TZ",
        is_global=False,
    )
    yield udf


def test_create_user_defined_function__default_catalog():
    """Test create_user_defined_function (default catalog)"""
    activate_catalog(None)
    with pytest.raises(RecordCreationException) as exc:
        UserDefinedFunction.create(
            name="udf_func",
            sql_function_name="cos",
            function_parameters=[],
            output_dtype="FLOAT",
            is_global=True,
        )

    expected_error = "Catalog not specified. Please specify a catalog."
    assert expected_error in str(exc.value)


def test_create_user_defined_function(catalog, cos_udf):
    """Test create_user_defined_function"""
    assert cos_udf.name == "cos_func"
    assert cos_udf.sql_function_name == "cos"
    assert cos_udf.output_dtype == "FLOAT"
    assert cos_udf.catalog_id is None
    assert cos_udf.feature_store_id == catalog.default_feature_store_ids[0]
    assert cos_udf.is_global is True
    assert cos_udf.function_parameters == [FunctionParameter(name="x", dtype="FLOAT")]

    # check the UDF class has the function now
    assert cos_udf.name in dir(UDF)

    # delete the UDF
    cos_udf.delete()

    # check the UDF class does not have the function anymore
    assert cos_udf.name not in dir(UDF)


def test_get__local_should_overwrite_global(cos_udf, local_cos_udf):
    """Test get user-defined function by name"""
    # CASE 1: both global & local UDFs exist
    udf = UserDefinedFunction.get(cos_udf.name)
    assert udf.id == local_cos_udf.id

    # check that the local UDF is used
    assert "cos_v2" in UDF.cos_func.__doc__

    # CASE 2: only global UDF exists
    # delete the local UDF
    local_cos_udf.delete()
    udf = UserDefinedFunction.get(cos_udf.name)
    assert udf.id == cos_udf.id

    # check that the global UDF is used
    assert "cos" in UDF.cos_func.__doc__ and "cos_v2" not in UDF.cos_func.__doc__

    # CASE 3: only local UDF exists
    # delete the global UDF & save local UDF again
    cos_udf.delete()
    local_cos_udf.save()
    udf = UserDefinedFunction.get(cos_udf.name)
    assert udf.id == local_cos_udf.id

    # check that the local UDF is used
    assert "cos_v2" in UDF.cos_func.__doc__

    # save the global UDF again, make sure the local UDF is still used
    cos_udf.save()
    udf = UserDefinedFunction.get(cos_udf.name)
    assert udf.id == local_cos_udf.id

    # check that the local UDF is used
    assert "cos_v2" in UDF.cos_func.__doc__


def test_list(cos_udf, power_udf, date_sub_udf):
    """Test list user-defined functions"""
    udf_list = UserDefinedFunction.list()
    pd.testing.assert_frame_equal(
        udf_list,
        pd.DataFrame({
            "id": [str(date_sub_udf.id), str(power_udf.id), str(cos_udf.id)],
            "signature": [date_sub_udf.signature, power_udf.signature, cos_udf.signature],
            "sql_function_name": [
                date_sub_udf.sql_function_name,
                power_udf.sql_function_name,
                cos_udf.sql_function_name,
            ],
            "feature_store_name": "sf_featurestore",
            "is_global": [date_sub_udf.is_global, power_udf.is_global, cos_udf.is_global],
        }),
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

    expected_error = "UserDefinedFunction is referenced by Feature: cos_feat"
    assert expected_error in str(exc.value)

    # delete the feature first then delete the UDF should succeed
    cos_feat.delete()
    cos_udf.delete()


def test_create_complex_feature_with_user_defined_function(
    power_udf, date_sub_udf, float_feature, non_time_based_feature
):
    """Test create a complex feature with user-defined function"""
    # user defined function with multiple feature inputs
    power_feat1 = UDF.power_func(float_feature, float_feature)
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
    expected_error = "UserDefinedFunction is referenced by Feature: power_feat2"
    assert expected_error in str(exc.value)

    with pytest.raises(RecordDeletionException) as exc:
        date_sub_udf.delete()
    expected_error = "UserDefinedFunction is referenced by Feature: date_sub_feat"
    assert expected_error in str(exc.value)

    # delete the feature first then delete the UDF should succeed (for power_udf)
    power_feat1.delete()
    power_feat2.delete()
    power_udf.delete()

    # delete the feature first then delete the UDF should succeed (for date_sub)
    date_sub_feat.delete()
    date_sub_udf.delete()


def test_create_feature_with_complex_operation(
    power_udf,
    cos_udf,
    snowflake_event_view_with_entity,
    feature_group_feature_job_setting,
    snowflake_event_table_id,
    update_fixtures,
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
    feat = UDF.cos_func(feat_group["sum_cos_float_square"])
    feat.name = "sum_cos_float_square"
    feat.save()

    # check feature definition
    check_sdk_code_generation(
        feat,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/feature_with_udf_used_in_view.py",
        update_fixtures=update_fixtures,
        event_table_id=snowflake_event_table_id,
        power_function_id=power_udf.id,
        cos_function_id=cos_udf.id,
    )

    # check feature model has expected properties
    assert sorted(feat.cached_model.user_defined_function_ids) == sorted([power_udf.id, cos_udf.id])

    # attempt to delete the UDF should fail
    expected_error = "UserDefinedFunction is referenced by Feature: sum_cos_float_square"
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


def test_create_feature_with_overriden_global_udf(cos_udf, local_cos_udf, float_feature):
    """Create a feature with overriden global UDF"""
    # check that local UDF (`cos_v2`) is used in UDF handler but not the global UDF (`cos`)
    assert f"`{local_cos_udf.sql_function_name}`" in UDF.cos_func.__doc__
    assert f"`{cos_udf.sql_function_name}`" not in UDF.cos_func.__doc__

    # create a new feature with the global UDF
    new_feat = cos_udf(float_feature)
    new_feat.name = "cos_feat"
    new_feat.save()

    # check that the feature definition contains the global UDF
    assert str(cos_udf.id) in new_feat.definition


def test_update_user_defined_function(cos_udf, float_feature):
    """Test update a user-defined function"""
    cos_udf.update_sql_function_name("cos_v3")
    assert cos_udf.sql_function_name == "cos_v3"

    func_params = [FunctionParameter(name="value", dtype="FLOAT")]
    cos_udf.update_function_parameters(func_params)
    assert cos_udf.function_parameters == func_params

    cos_udf.update_output_dtype("INT")
    assert cos_udf.output_dtype == "INT"

    # attempt to update the UDF with no changes should fail
    with pytest.raises(RecordUpdateException) as exc:
        cos_udf.update_output_dtype("INT")
    assert "No changes detected in user defined function" in str(exc.value)

    # attempt to update the UDF used by a saved feature should fail
    new_feat = cos_udf(float_feature)
    new_feat.name = "new_feat"
    new_feat.save()

    with pytest.raises(RecordUpdateException) as exc:
        cos_udf.update_output_dtype("FLOAT")
    expected_error = "UserDefinedFunction is referenced by Feature: new_feat"
    assert expected_error in str(exc.value)


def test_create_feature_with_deleted_user_defined_function(cos_udf, float_feature):
    """Test save a feature with deleted user-defined function should fail"""
    new_feat = cos_udf(float_feature)
    new_feat.name = "new_feat"
    cos_udf.delete()

    with pytest.raises(RecordCreationException) as exc:
        new_feat.save()

    expected_error = "Please save the UserDefinedFunction object first."
    assert expected_error in str(exc.value)


def test_update_description(cos_udf):
    """Test update description"""
    assert cos_udf.description is None
    cos_udf.update_description("new description")
    assert cos_udf.description == "new description"
    assert cos_udf.info()["description"] == "new description"
    cos_udf.update_description(None)
    assert cos_udf.description is None
    assert cos_udf.info()["description"] is None


def test_deployment_enablement_for_udf_feat(
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    snowflake_scd_view_with_entity,
    cos_udf,
    power_udf,
    date_sub_udf,
    mock_api_object_cache,
):
    """Test deployment enablement for feature with UDF"""
    _ = mock_update_data_warehouse, mock_offline_store_feature_manager_dependencies
    _ = cos_udf, power_udf, date_sub_udf
    _ = mock_api_object_cache
    float_feat = snowflake_scd_view_with_entity.col_float.as_feature("float_feat")
    cos_feat = UDF.cos_func(float_feat)

    ts_feat = snowflake_scd_view_with_entity.end_timestamp.as_feature("ts_feat")
    date_sub_feat = UDF.date_sub_func(ts_feat, 10)
    date_sub_feat.name = "date_sub_feat"

    power_cos_feat = UDF.power_func(cos_feat, 2)
    power_cos_feat.name = "power_cos_feat"

    feature_list = FeatureList([power_cos_feat, date_sub_feat], name="my_feature_list")
    feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()

    # check deployment is enabled
    assert deployment.enabled is True


def test_udf_feat_with_on_demand_function(
    snowflake_scd_view_with_entity,
    snowflake_event_view_with_entity,
    power_udf,
    float_feature,
    non_time_based_feature,
):
    """Test deployment enablement for feature with UDF (when on-demand function will be required)"""

    _ = power_udf
    ts_feat = snowflake_scd_view_with_entity.end_timestamp.as_feature("ts_feat")
    col_feat = snowflake_scd_view_with_entity.col_float.as_feature("col_feat")
    feat_with_req_col = (RequestColumn.point_in_time() - ts_feat).dt.day
    feat_with_diff_entity = snowflake_event_view_with_entity.groupby(["col_int"]).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1d"],
        feature_names=["sum_1d_diff_entity"],
        feature_job_setting=float_feature.table_id_feature_job_settings[0].feature_job_setting,
    )["sum_1d_diff_entity"]

    expected_error = (
        'Error in feature #2 ("None"): This feature was created with a request column and cannot be used as '
        "an input to this function. Please change the feature and try again."
    )
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        UDF.power_func(col_feat, feat_with_req_col)

    expected_error = (
        "This feature requires a Python on-demand function during deployment. "
        "We cannot proceed with creating the feature because the on-demand function involves a UDF, "
        "and the Python version of the UDF is not supported at the moment."
    )
    transformed_feat = UDF.power_func(col_feat, float_feature)
    transformed_feat.name = "transformed_feat"
    with pytest.raises(Exception, match=re.escape(expected_error)):
        transformed_feat.save()

    # this is ok as the two features' primary entities can be joined
    another_transformed_feat = UDF.power_func(float_feature, feat_with_diff_entity)
    another_transformed_feat.name = "another_transformed_feat"
    another_transformed_feat.save()
