"""
Test custom_nav module
"""

from featurebyte.common.documentation.custom_nav import BetaWave3Nav
from featurebyte.common.documentation.documentation_layout import get_overall_layout


def test_get_items_for_level():
    """
    Test _get_items_for_level
    """
    ordering = ["a", "c", "b"]
    data = {
        "d": "d_description",  # item not included in the ordering should be at the bottom
        "b": "b_description",
        "a": "a_description",
        "c": "c_description",
    }
    sorted_items = BetaWave3Nav._get_items_for_level(data, ordering)
    assert list(sorted_items) == [
        ("a", "a_description"),
        ("c", "c_description"),
        ("b", "b_description"),
        ("d", "d_description"),
    ]


def test_all_keys_present_in_root_level_order():
    """
    Test that all root level keys are present in _custom_root_level_order.

    If this test fails, it is likely that we will need to add a new key to _custom_root_level_order to indicate
    what ordering we want it to appear in.
    """
    all_root_keys = set()
    for item in get_overall_layout():
        all_root_keys.add(item.menu_header[0])

    currently_mapped_keys = set(BetaWave3Nav._custom_root_level_order)
    assert all_root_keys == currently_mapped_keys, (
        "If this test fails, it is likely that we will need to add a "
        "new key to _custom_root_level_order to indicate what ordering "
        "we want it to appear in."
    )


def test_all_keys_present_in_second_level_order():
    """
    Test that all root level keys are present in _custom_second_level_order

    If this test fails, it is likely that we will need to add a new key to _custom_second_level_order to indicate
    what ordering we want it to appear in.
    """
    all_second_level_keys = set()
    for item in get_overall_layout():
        menu_header = item.menu_header
        if len(menu_header) > 1:
            all_second_level_keys.add(menu_header[1])

    currently_mapped_keys = set(BetaWave3Nav._custom_second_level_order)
    assert all_second_level_keys == currently_mapped_keys, (
        "If this test fails, it is likely that we will need to "
        "add a new key to _custom_root_level_order to indicate "
        "what ordering we want it to appear in."
    )
