import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from featurebyte import AggFunc, FeatureList


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_expected_rows_and_columns(item_data, expected_joined_event_item_dataframe):
    """
    Test ItemView rows and columns are correct
    """
    item_view = item_data.get_view()
    df_preview = item_view.preview(limit=50)
    expected_columns = [
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]
    assert df_preview.columns.tolist() == expected_columns
    assert df_preview.shape[0] == 50

    # Check preview result with the expected joined events-items data
    df_expected_subset = pd.merge(
        df_preview[["order_id", "item_id"]],
        expected_joined_event_item_dataframe,
        left_on=["order_id", "item_id"],
        right_on=["order_id", "item_id"],
        how="left",
    )
    df_expected_subset = df_expected_subset[expected_columns]
    pd.testing.assert_frame_equal(df_preview, df_expected_subset)


@pytest.fixture
def item_aggregate_with_category_features(item_data):
    """
    Fixture for a FeatureList with features derived from item aggregation per category
    """
    item_view = item_data.get_view()
    feature = item_view.groupby("order_id", category="item_type").aggregate(
        method=AggFunc.COUNT, feature_name="my_item_feature"
    )
    most_frequent_feature = feature.cd.most_frequent()
    most_frequent_feature.name = "most_frequent_item_type"
    entropy_feature = feature.cd.entropy()
    entropy_feature.name = "item_type_entropy"
    return FeatureList([most_frequent_feature, entropy_feature], name="feature_list")


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_aggregation_with_category(item_aggregate_with_category_features, event_data):
    """
    Test ItemView.groupby(..., category=...).aggregate() feature
    """
    # check preview
    df = item_aggregate_with_category_features.preview(pd.DataFrame([{"order_id": "T42"}]))
    assert df.iloc[0].to_dict() == {
        "order_id": "T42",
        "most_frequent_item_type": "type_21",
        "item_type_entropy": 1.791759469228055,
    }

    # check historical features
    df = item_aggregate_with_category_features.get_historical_features(
        pd.DataFrame(
            {
                "POINT_IN_TIME": ["2001-11-15 10:00:00"] * 3,
                "order_id": [f"T{i}" for i in range(3)],
            }
        )
    )
    df = df.sort_values("order_id")
    assert df["most_frequent_item_type"].tolist() == ["type_2", "type_18", "type_13"]
    np.testing.assert_allclose(df["item_type_entropy"].values, [1.79175947, 1.09861229, 2.19722458])

    # check add_feature (note added feature value is the same as the preview above)
    event_view = event_data.get_view()
    event_view.add_feature(
        "most_frequent_item_type",
        item_aggregate_with_category_features["most_frequent_item_type"],
        "TRANSACTION_ID",
    )
    df = event_view[event_view["TRANSACTION_ID"] == "T42"].preview()
    assert df.iloc[0]["most_frequent_item_type"] == "type_21"


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_view_ops(item_data, expected_joined_event_item_dataframe):
    """
    Test ItemView operations
    """
    item_view = item_data.get_view()

    # Add a new column
    item_view["item_type_upper"] = item_view["item_type"].str.upper()

    # Filter on a column
    item_view_filtered = item_view[item_view["item_type_upper"] == "TYPE_42"]
    df = item_view_filtered.preview(500)
    assert (df["item_type_upper"] == "TYPE_42").all()

    # Test previewing a temporary column
    df = (item_view_filtered["item_type_upper"] + "_ABC").preview()
    assert (df.iloc[:, 0] == "TYPE_42_ABC").all()

    # Join additional columns from EventTable
    item_view_filtered.join_event_data_attributes(["SESSION_ID"])
    df = item_view_filtered.preview(500)
    assert df["SESSION_ID"].notnull().all()
    assert (df["item_type_upper"] == "TYPE_42").all()

    # Create a feature using aggregation with time windows and preview it
    feature = item_view_filtered.groupby("ÜSER ID", category="item_type_upper").aggregate_over(
        method="count",
        windows=["30d"],
        feature_names=["count_30d"],
    )["count_30d"]
    df = feature.preview(pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}]))
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "üser id": 1,
        "count_30d": '{\n  "TYPE_42": 4\n}',
    }
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-11-15 10:00:00"] * 10),
            "üser id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList([feature], name="feature_list")
    df_historical_features = feature_list.get_historical_features(df_training_events)
    assert df_historical_features["count_30d"].tolist() == [
        '{\n  "TYPE_42": 4\n}',
        None,
        '{\n  "TYPE_42": 2\n}',
        '{\n  "TYPE_42": 2\n}',
        '{\n  "TYPE_42": 5\n}',
        '{\n  "TYPE_42": 4\n}',
        '{\n  "TYPE_42": 2\n}',
        '{\n  "TYPE_42": 3\n}',
        '{\n  "TYPE_42": 2\n}',
        None,
    ]

    # Create a feature using aggregation without time window and preview it
    feature = item_view_filtered.groupby("order_id").aggregate(
        method=AggFunc.COUNT,
        feature_name="order_size",
    )
    df = feature.preview(pd.DataFrame([{"order_id": "T30"}]))
    assert df.iloc[0].to_dict() == {"order_id": "T30", "order_size": 1}


def assert_match(item_id: str, item_name: str, item_type: str):
    """
    Helper method to assert values in the joined table.
    """
    id_str = item_id.lstrip("item")
    # The format of these expected values are defined in the fixture setup of the dimension view dataframe.
    expected_name = f"name{id_str}"
    expected_type = f"type{id_str}"
    assert item_name == expected_name
    assert item_type == expected_type


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_view_joined_with_dimension_view(
    transaction_data_upper_case, item_data, dimension_data
):
    """
    Test joining an item view with a dimension view.
    """
    # create item view
    item_view = item_data.get_view()
    item_columns = [
        "order_id",
        "item_id",
        "item_type",
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
    ]
    assert item_view.columns == item_columns
    original_item_preview = item_view.preview()

    # create dimension view
    dimension_view = dimension_data.get_view()
    initial_dimension_columns = ["created_at", "item_id", "item_name", "item_type"]
    assert dimension_view.columns == initial_dimension_columns

    # perform the join
    suffix = "_dimension"
    item_view.join(dimension_view, rsuffix=suffix)

    # assert columns are updated after the join
    filtered_dimension_columns = [
        "created_at",
        "item_name",
        "item_type",
    ]  # no item_id since join key is removed
    item_columns.extend([f"{col}{suffix}" for col in filtered_dimension_columns])
    item_preview = item_view.preview()
    assert item_preview.columns.tolist() == item_columns
    number_of_elements = original_item_preview.shape[0]
    # Verify that we have non-zero number of elements to represent a successful join
    assert number_of_elements > 0
    assert item_preview.shape[0] == number_of_elements

    # Verify that the item_id's are the same
    assert_series_equal(item_preview["item_id"], original_item_preview["item_id"])

    # verify that the values in the joined columns are as we expect
    for _, row in item_preview.iterrows():
        curr_item_id = row["item_id"]
        joined_item_name = row["item_name_dimension"]
        joined_item_type = row["item_type_dimension"]
        assert_match(curr_item_id, joined_item_name, joined_item_type)

    # check historical features
    feature = (
        item_view.groupby("ÜSER ID", category="item_type_dimension")
        .aggregate_over(
            method="count",
            windows=["30d"],
            feature_names=["count_30d"],
        )["count_30d"]
        .cd.most_frequent()
    )
    feature.name = "most_frequent_item_type_30d"
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00"] * 5),
            "üser id": [1, 2, 3, 4, 5],
        }
    )
    feature_list = FeatureList([feature], name="feature_list")
    df_historical_features = feature_list.get_historical_features(df_training_events)
    assert df_historical_features.sort_values("üser id")[
        "most_frequent_item_type_30d"
    ].tolist() == ["type_10140", "type_1008", "type_11333", "type_10261", "type_10657"]
