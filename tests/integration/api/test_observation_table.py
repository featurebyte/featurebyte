"""
Integration tests for ObservationTable
"""

import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from featurebyte import (
    AssignmentDesign,
    AssignmentSource,
    Context,
    Propensity,
    TargetNamespace,
    TargetValueSamplingRate,
    Treatment,
    TreatmentInterference,
    TreatmentTime,
    TreatmentTimeStructure,
    TreatmentType,
    UseCase,
)
from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.enum import DBVarType, SpecialColumnName, TargetType
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import DownSamplingInfo, RequestInputType
from tests.integration.api.materialized_table.utils import (
    check_location_valid,
    check_materialized_table_accessible,
    check_materialized_table_preview_methods,
)


@pytest.fixture(name="normal_user_id_entity", scope="session")
def new_user_id_entity_fixture():
    """
    Fixture for a new user id entity
    """
    entity = Entity(name="normal user id", serving_names=["User ID"])
    entity.save()
    return entity


@pytest.fixture(name="user_use_case")
def user_use_case_fixture(event_view, user_entity):
    """
    Fixture for a user use case
    """
    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="avg",
        value_column="ÀMOUNT",
        window="24h",
        target_name="user_avg_24h_target",
        fill_value=None,
        target_type=TargetType.REGRESSION,
    )
    target.save()
    context = Context.create(name="user_context", primary_entity=[user_entity.name])
    use_case = UseCase.create(
        name="user_use_case", target_name=target.name, context_name=context.name
    )
    return use_case


@pytest.fixture(name="customer_use_case")
def customer_use_case_fixture(event_view, customer_entity):
    """
    Fixture for a customer use case
    """
    target = event_view.groupby("CUST_ID").forward_aggregate(
        method="avg",
        value_column="ÀMOUNT",
        window="24h",
        target_name="cust_avg_24h_target",
        fill_value=None,
        target_type=TargetType.REGRESSION,
    )
    target.save()
    context = Context.create(name="customer_context", primary_entity=[customer_entity.name])
    use_case = UseCase.create(
        name="customer_use_case", target_name=target.name, context_name=context.name
    )
    return use_case


@pytest.fixture(name="user_classification_use_case")
def user_classification_use_case_fixture(event_view, user_entity):
    """
    Fixture for a user classification use case
    """
    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="avg",
        value_column="ÀMOUNT",
        window="24h",
        target_name="user_avg_24h_target",
        fill_value=0,
    )
    target = target > 0
    target.name = "user_active_24h_target"
    target.save()
    target.update_target_type(TargetType.CLASSIFICATION)
    target.update_positive_label(True)
    context = Context.create(name="user_classification_context", primary_entity=[user_entity.name])
    use_case = UseCase.create(
        name="user_classification_use_case", target_name=target.name, context_name=context.name
    )
    return use_case


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_from_source_table(
    data_source, feature_store, session, source_type, catalog, normal_user_id_entity
):
    """
    Test creating an observation table from a source table
    """
    _ = normal_user_id_entity
    source_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="ORIGINAL_OBSERVATION_TABLE",
    )
    sample_rows = 123
    observation_table = source_table.create_observation_table(
        f"MY_OBSERVATION_TABLE_{source_type}", sample_rows=sample_rows
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_{source_type}"
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    user_id_entity_col_name = "User ID"
    check_materialized_table_preview_methods(
        observation_table, expected_columns=["POINT_IN_TIME", user_id_entity_col_name]
    )
    assert observation_table.least_recent_point_in_time is not None
    assert "User ID" in observation_table.entity_column_name_to_count
    df = observation_table.to_pandas()
    expected_min = df["POINT_IN_TIME"].min()
    expected_max = df["POINT_IN_TIME"].max()
    expected_cust_id_unique_count = df[user_id_entity_col_name].nunique()
    assert observation_table.entity_column_name_to_count["User ID"] == expected_cust_id_unique_count
    assert df.columns.tolist() == ["POINT_IN_TIME", user_id_entity_col_name]

    def _convert_timestamp_for_timezones(timestamp_str):
        current_timestamp = pd.Timestamp(timestamp_str)
        if current_timestamp.tzinfo is not None:
            current_timestamp = current_timestamp.tz_convert("UTC").tz_localize(None)
        current_timestamp = current_timestamp.isoformat()
        return str(current_timestamp)

    assert _convert_timestamp_for_timezones(
        observation_table.least_recent_point_in_time
    ) == _convert_timestamp_for_timezones(str(expected_min))
    assert _convert_timestamp_for_timezones(
        observation_table.most_recent_point_in_time
    ) == _convert_timestamp_for_timezones(str(expected_max))

    # test create observation table with from observation table
    new_sampled_rows = 50
    new_observation_table = observation_table.create_observation_table(
        f"MY_OBSERVATION_TABLE_2_{source_type}", sample_rows=new_sampled_rows
    )
    assert new_observation_table.name == f"MY_OBSERVATION_TABLE_2_{source_type}"
    table_details = new_observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, new_sampled_rows)


@pytest.mark.asyncio
async def test_observation_table_min_interval_between_entities(
    catalog, session, data_source, normal_user_id_entity
):
    _ = catalog, normal_user_id_entity
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "fixtures", "observation_table_time_interval.csv")
    )
    df[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
        df[SpecialColumnName.POINT_IN_TIME].astype(str)
    )
    df["treatment_test"] = np.tile([0, 1], len(df) // 2 + 1)[: len(df)]

    table_name = "observation_table_time_interval"
    await session.register_table(table_name, df)

    # prepare the observation table
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    sample_rows = 123

    # create a target namespace
    target_name = "Target"
    target_namespace = TargetNamespace.create(
        name=target_name,
        primary_entity=[],
        dtype=DBVarType.VARCHAR,
        target_type=TargetType.CLASSIFICATION,
    )

    treatment_name = "treatment_test"
    treatment = Treatment.create(
        name=treatment_name,
        dtype=DBVarType.INT,
        treatment_type=TreatmentType.BINARY,
        source=AssignmentSource.RANDOMIZED,
        design=AssignmentDesign.SIMPLE_RANDOMIZATION,
        time=TreatmentTime.STATIC,
        time_structure=TreatmentTimeStructure.INSTANTANEOUS,
        interference=TreatmentInterference.NONE,
        treatment_labels=[0, 1],
        control_label=0,
        propensity=Propensity(
            granularity="global",
            knowledge="design-known",
            p_global=0.5,
        ),
    )

    # create the observation table with target column
    observation_table = database_table.create_observation_table(
        "MY_OBSERVATION_TABLE_FOR_INTERVALS",
        sample_rows=sample_rows,
        target_column=target_name,
        treatment_column=treatment_name,
    )
    assert observation_table.min_interval_secs_between_entities == 3600
    assert observation_table.target_namespace.id == target_namespace.id
    assert observation_table.treatment.id == treatment.id

    # check missing data table
    table_with_missing_data = observation_table.cached_model.table_with_missing_data
    missing_data_table = data_source.get_source_table(
        database_name=table_with_missing_data.database_name,
        schema_name=table_with_missing_data.schema_name,
        table_name=table_with_missing_data.table_name,
    )
    missing_data_table_df = missing_data_table.preview()
    assert missing_data_table_df.shape[0] == 1
    pd.testing.assert_frame_equal(
        missing_data_table_df,
        pd.DataFrame({
            "POINT_IN_TIME": [pd.Timestamp("2022-06-23 00:58:00")],
            "User ID": [2],
            "Target": [np.nan],
            "treatment_test": [1],
        }),
    )

    # check target namespace document
    target_namespace = target_namespace.get_by_id(id=target_namespace.id)
    target_namespace_model = target_namespace.cached_model
    positive_label_candidates = target_namespace_model.positive_label_candidates
    assert len(positive_label_candidates) == 1
    assert positive_label_candidates[0].observation_table_id == observation_table.id
    assert sorted(positive_label_candidates[0].positive_label_candidates) == [
        "Not Verified",
        "Verified",
    ]

    # delete the observation table & target
    observation_table.delete()
    target_namespace.delete()
    treatment.delete()

    # attempt to preview the missing data table should raise an error
    with pytest.raises(RecordRetrievalException):
        missing_data_table.preview()


@pytest.mark.asyncio
async def test_observation_table_from_view(
    event_table, scd_table, session, source_type, user_entity, user_use_case
):
    """
    Test creating an observation table from a view
    """
    _ = user_entity
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME", "ÜSER ID": "üser id"},
        use_case_name=user_use_case.name,
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}"
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    # expect target column to be automatically included
    check_materialized_table_preview_methods(
        observation_table,
        expected_columns=["POINT_IN_TIME", "üser id", "user_avg_24h_target"],
    )

    # check missing data table is None
    assert observation_table.cached_model.table_with_missing_data is None

    # check point in time column is converted to timestamp type
    point_in_time_info = next(
        col for col in observation_table.columns_info if col.name == "POINT_IN_TIME"
    )
    assert point_in_time_info.dtype == DBVarType.TIMESTAMP


@pytest.mark.asyncio
async def test_observation_table_cleanup(scd_table, session, source_type):
    """
    Test that invalid observation tables are cleaned up
    """

    async def _get_num_observation_tables():
        tables = await session.list_tables(
            database_name=session.database_name, schema_name=session.schema_name
        )
        observation_table_names = [
            table.name for table in tables if table.name.startswith("OBSERVATION_TABLE")
        ]
        return len(observation_table_names)

    view = scd_table.get_view()
    view["POINT_IN_TIME"] = 123

    num_observation_tables_before = await _get_num_observation_tables()

    with pytest.raises(RecordCreationException) as exc:
        view.create_observation_table(
            f"BAD_OBSERVATION_TABLE_FROM_VIEW_{source_type}",
            primary_entities=["User"],
            columns_rename_mapping={"User ID": "üser id"},
        )

    expected_msg = "Point in time column should have timestamp type; got INT"
    assert expected_msg in str(exc.value)

    num_observation_tables_after = await _get_num_observation_tables()
    assert num_observation_tables_before == num_observation_tables_after


@pytest.mark.parametrize("file_type", ["csv", "parquet"])
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_upload(
    feature_store, catalog, customer_entity, session, source_type, file_type
):
    _ = catalog, customer_entity

    # Upload observation table
    file_name = f"observation_table.{file_type}"
    file_path = os.path.join(os.path.dirname(__file__), "fixtures", file_name)
    if file_type == "csv":
        df = pd.read_csv(file_path)
    elif file_type == "parquet":
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    target_name = "Target"
    target_namespace = TargetNamespace.create(
        name=target_name,
        primary_entity=[],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )

    number_of_rows = df[target_name].dropna().shape[0]
    observation_table = ObservationTable.upload(
        file_path=file_path,
        name=f"uploaded_observation_table_{file_type}",
        target_column=target_name,
    )

    # check existence of table with missing data
    table_with_missing_data = observation_table.cached_model.table_with_missing_data
    assert table_with_missing_data is not None

    data_source = feature_store.get_data_source()
    missing_data_table = data_source.get_source_table(
        database_name=table_with_missing_data.database_name,
        schema_name=table_with_missing_data.schema_name,
        table_name=table_with_missing_data.table_name,
    )
    missing_data_table_df = missing_data_table.preview()
    assert missing_data_table_df[target_name].isna().all()
    assert missing_data_table_df.shape[0] == 1

    # Assert response
    assert observation_table.name == f"uploaded_observation_table_{file_type}"
    assert observation_table.request_input == UploadedFileInput(
        type=RequestInputType.UPLOADED_FILE,
        file_name=file_name,
    )
    expected_columns = {SpecialColumnName.POINT_IN_TIME, "cust_id", target_name}
    actual_columns = {column.name for column in observation_table.columns_info}
    assert expected_columns == actual_columns

    # Assert materialized table
    await check_materialized_table_accessible(
        observation_table.location.table_details, session, source_type, number_of_rows
    )
    check_materialized_table_preview_methods(
        observation_table, [SpecialColumnName.POINT_IN_TIME, "cust_id", target_name], number_of_rows
    )

    # delete the observation table & target
    observation_table.delete()
    target_namespace.delete()


@pytest.mark.asyncio
async def test_observation_table_sample_time_range(
    event_table, scd_table, session, source_type, user_entity
):
    """
    Test creating an observation table from a view sampled with time range
    """
    _ = user_entity
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}_TIME_RANGE_SAMPLED",
        sample_rows=sample_rows,
        sample_from_timestamp="2001-02-01T00:00:00Z",
        sample_to_timestamp="2001-06-30T00:00:00Z",
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME", "ÜSER ID": "üser id"},
    )
    assert (
        observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}_TIME_RANGE_SAMPLED"
    )
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    df_preview = observation_table.preview(limit=10)
    assert df_preview.columns.tolist() == ["POINT_IN_TIME", "üser id"]

    df_describe = observation_table.describe()
    assert pd.to_datetime(df_describe.loc["min", "POINT_IN_TIME"]) >= pd.Timestamp("2001-02-01")
    assert pd.to_datetime(df_describe.loc["max", "POINT_IN_TIME"]) <= pd.Timestamp("2001-06-30")


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_upload_no_target(
    feature_store, catalog, customer_entity, session, source_type, customer_use_case
):
    _ = catalog, customer_entity

    # Upload observation table
    file_name = "observation_table_no_target.csv"
    file_path = os.path.join(os.path.dirname(__file__), "fixtures", file_name)
    df = pd.read_csv(file_path)

    number_of_rows = df.shape[0]
    observation_table = ObservationTable.upload(
        file_path=file_path,
        name="uploaded_observation_table",
        use_case_name=customer_use_case.name,
    )

    # no missing data
    table_with_missing_data = observation_table.cached_model.table_with_missing_data
    assert table_with_missing_data is None

    # Assert response
    assert observation_table.name == "uploaded_observation_table"
    assert observation_table.request_input == UploadedFileInput(
        type=RequestInputType.UPLOADED_FILE,
        file_name=file_name,
    )

    # expect target column to be automatically included
    expected_columns = {SpecialColumnName.POINT_IN_TIME, "cust_id", "cust_avg_24h_target"}
    actual_columns = {column.name for column in observation_table.columns_info}
    assert expected_columns == actual_columns

    # Assert materialized table
    await check_materialized_table_accessible(
        observation_table.location.table_details, session, source_type, number_of_rows
    )
    check_materialized_table_preview_methods(
        observation_table,
        [SpecialColumnName.POINT_IN_TIME, "cust_id", "cust_avg_24h_target"],
        number_of_rows,
    )

    # delete the observation table
    customer_use_case.remove_observation_table(observation_table.name)
    observation_table.delete()


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_downsampling(
    event_table, scd_table, session, source_type, user_entity, user_classification_use_case
):
    _ = user_entity
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    source_observation_table = view.create_observation_table(
        f"SOURCE_OBSERVATION_TABLE_FOR_DOWNSAMPLING_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME", "ÜSER ID": "üser id"},
        use_case_name=user_classification_use_case.name,
    )
    assert (
        source_observation_table.name == f"SOURCE_OBSERVATION_TABLE_FOR_DOWNSAMPLING_{source_type}"
    )
    table_details = source_observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    df_describe = source_observation_table.describe()
    assert df_describe.loc["top", "user_active_24h_target"] == "true"
    pos_obs_count = df_describe.loc["freq", "user_active_24h_target"]
    assert source_observation_table.shape()[0] == 123
    neg_obs_count = 123 - pos_obs_count

    # create downsampled observation table
    observation_table = source_observation_table.create_observation_table(
        name=f"SOURCE_OBSERVATION_TABLE_FOR_DOWNSAMPLING_{source_type}_DOWNSAMPLED",
        downsampling_info=DownSamplingInfo(
            sampling_rate_per_target_value=[
                # downsample positive class to 50%
                TargetValueSamplingRate(target_value=True, rate=0.5),
            ],
        ),
    )
    df_describe = observation_table.describe()
    assert df_describe.loc["top", "user_active_24h_target"] == "true"
    pos_obs_count_after_downsampling = df_describe.loc["freq", "user_active_24h_target"]
    assert pos_obs_count_after_downsampling < pos_obs_count * 0.65  # allow some randomness
    assert pos_obs_count_after_downsampling > pos_obs_count * 0.35  # allow some randomness
    number_of_rows = observation_table.shape()[0]
    assert number_of_rows == pos_obs_count_after_downsampling + neg_obs_count
    check_materialized_table_preview_methods(
        observation_table,
        [
            SpecialColumnName.POINT_IN_TIME,
            "üser id",
            "user_active_24h_target",
            "__FB_TABLE_ROW_WEIGHT",
        ],
        number_of_rows,
    )
    table_details = observation_table.location.table_details
    sampling_rates = await session.execute_query(
        f'SELECT DISTINCT __FB_TABLE_ROW_WEIGHT FROM "{table_details.database_name}"."{table_details.schema_name}"."{table_details.table_name}"'
    )
    assert set(sampling_rates["__FB_TABLE_ROW_WEIGHT"].tolist()) == {2.0, 1.0}

    # create double downsampled observation table
    observation_table = observation_table.create_observation_table(
        name=f"SOURCE_OBSERVATION_TABLE_FOR_DOWNSAMPLING_{source_type}_DOWNSAMPLED_TWICE",
        downsampling_info=DownSamplingInfo(
            sampling_rate_per_target_value=[
                # downsample positive class to 30%
                TargetValueSamplingRate(target_value=True, rate=0.5),
            ],
        ),
    )
    df_describe = observation_table.describe()
    assert df_describe.loc["top", "user_active_24h_target"] == "true"
    pos_obs_count = pos_obs_count_after_downsampling
    pos_obs_count_after_downsampling = df_describe.loc["freq", "user_active_24h_target"]
    assert pos_obs_count_after_downsampling < pos_obs_count * 0.65  # allow some randomness
    assert pos_obs_count_after_downsampling > pos_obs_count * 0.35  # allow some randomness
    number_of_rows = observation_table.shape()[0]
    assert number_of_rows == pos_obs_count_after_downsampling + neg_obs_count
    check_materialized_table_preview_methods(
        observation_table,
        [
            SpecialColumnName.POINT_IN_TIME,
            "üser id",
            "user_active_24h_target",
            "__FB_TABLE_ROW_WEIGHT",
        ],
        number_of_rows,
    )
    table_details = observation_table.location.table_details
    sampling_rates = await session.execute_query(
        f'SELECT DISTINCT __FB_TABLE_ROW_WEIGHT FROM "{table_details.database_name}"."{table_details.schema_name}"."{table_details.table_name}"'
    )
    assert set(sampling_rates["__FB_TABLE_ROW_WEIGHT"].tolist()) == {4.0, 1.0}
    # ensure weight column is not included in columns_info
    expected_columns = {
        SpecialColumnName.POINT_IN_TIME,
        "üser id",
        "user_active_24h_target",
    }
    actual_columns = {column.name for column in observation_table.columns_info}
    assert expected_columns == actual_columns

    # check row weight column is included in the downloaded parquet file
    with tempfile.TemporaryDirectory() as tmpdir:
        download_path = os.path.join(tmpdir, "downloaded_observation_table.parquet")
        observation_table.download(download_path)
        downloaded_df = pd.read_parquet(download_path)
        assert "__FB_TABLE_ROW_WEIGHT" in downloaded_df.columns
