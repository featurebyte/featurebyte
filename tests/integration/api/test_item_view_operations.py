import pytest

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
        "order_id",
        "item_id",
        "item_type",
    ]
    assert df_preview.shape[0] == 50

    # Check preview result with the expected joined events-items data
    for _, row in df_preview.iterrows():
        # Check if each row in the preview result appears in the expected joined DataFrame
        mask = expected_joined_event_item_dataframe["EVENT_TIMESTAMP"] == row["EVENT_TIMESTAMP"]
        for col in ["USER ID", "order_id", "item_id", "item_type"]:
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
