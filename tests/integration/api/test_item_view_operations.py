import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.item_view import ItemView


@pytest.mark.parametrize(
    "item_data",
    ["snowflake"],
    indirect=True,
)
def test_expected_rows_and_columns(item_data, expected_joined_event_item_dataframe):
    """
    Test ItemView rows and columns are correct
    """
    item_view = ItemView.from_item_data(item_data)
    df_preview = item_view.preview(limit=50)
    assert df_preview.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "USER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]
    assert df_preview.shape[0] == 50

    # Check preview result with the expected joined events-items data
    for _, row in df_preview.iterrows():
        # Check if each row in the preview result appears in the expected joined DataFrame
        mask = expected_joined_event_item_dataframe["EVENT_TIMESTAMP"] == row["EVENT_TIMESTAMP"]
        for col in ["USER ID", "PRODUCT_ACTION", "order_id", "item_id", "item_type"]:
            mask &= expected_joined_event_item_dataframe[col] == row[col]
        matched = expected_joined_event_item_dataframe[mask]
        assert matched.shape[0] == 1, f"Preview row {row.to_dict()} not found"


def test_item_view_operations(item_data):
    """
    Test ItemView operations
    """
    item_view = ItemView.from_item_data(item_data)

    # Add a new column
    item_view["item_type_upper"] = item_view["item_type"].str.upper()

    # Filter on a column
    item_view_filtered = item_view[item_view["item_type_upper"] == "TYPE_42"]
    df = item_view_filtered.preview(500)
    assert (df["item_type_upper"] == "TYPE_42").all()

    # Join additional columns from EventData
    item_view_filtered.join_event_data_attributes(["SESSION_ID"])
    df = item_view_filtered.preview(500)
    assert df["SESSION_ID"].notnull().all()
    assert (df["item_type_upper"] == "TYPE_42").all()

    # Create a feature using aggregation with time windows and preview it
    feature = item_view_filtered.groupby("USER ID", category="item_type_upper").aggregate_over(
        method="count",
        windows=["30d"],
        feature_names=["count_30d"],
    )["count_30d"]
    df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "user id": 1})
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "user id": 1,
        "count_30d": '{\n  "TYPE_42": 2\n}',
    }

    # Create a feature using aggregation without time window and preview it
    feature = item_view_filtered.groupby("order_id").aggregate(
        method="count",
        feature_name="order_size",
    )
    df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "order_id": "T236"})
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "order_id": "T236",
        "order_size": 1,
    }


@pytest.mark.parametrize(
    "item_data",
    ["snowflake"],
    indirect=True,
)
def test_item_view_joined_with_dimension_view(
    transaction_data_upper_case, item_data, dimension_data
):
    """
    Test joining an item view with a dimension view.
    """
    # create item view
    item_view = ItemView.from_item_data(item_data)
    item_columns = [
        "order_id",
        "item_id",
        "item_type",
        "EVENT_TIMESTAMP",
        "USER ID",
        "PRODUCT_ACTION",
    ]
    assert item_view.columns == item_columns
    original_item_preview = item_view.preview()

    # create dimension view
    dimension_view = DimensionView.from_dimension_data(dimension_data)
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
    # Verify that the item_type in the joined view, is the same as the original item_type in the dimension view
    dimension_preview = dimension_view.preview()

    # compare the result of our join, against a pandas join
    joined_frame = item_preview.join(dimension_preview, rsuffix=suffix)
    assert_series_equal(item_preview["item_id"], joined_frame["item_id"])
    assert_series_equal(item_preview["item_name_dimension"], joined_frame["item_name_dimension"])
