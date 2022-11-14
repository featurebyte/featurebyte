import pytest

from featurebyte.api.item_view import ItemView


@pytest.mark.parametrize(
    "item_data",
    ["snowflake"],
    indirect=True,
)
def test_item_view_operations(item_data):
    """
    Test ItemView operations
    """
    item_view = ItemView.from_item_data(item_data)
    df_preview = item_view.preview(limit=10)
    assert df_preview.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "USER ID",
        "order_id",
        "item_id",
        "item_type",
    ]
    assert df_preview.shape[0] == 10
