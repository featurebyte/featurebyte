"""
Doc layout test module
"""
import featurebyte
from featurebyte.common.documentation.documentation_layout import get_overall_layout


def test_all_init_items_are_exposed():
    """
    Test that all methods in __init__ are exposed in the docs.
    """
    all_exposed_items = featurebyte.__all__
    overall_layout = get_overall_layout()
    items = set()
    for item in overall_layout:
        items.add(item.menu_header[-1])  # Add the method/property/field.
        items.add(item.menu_header[0])  # Add top level nav item, typically the class.
    missing_items = []
    # excluded_items are items that are in __init__ but are not exposed in the docs.
    # Add to this list if you add a new item to __init__, but do not want to expose it in the docs for whatever
    # reason.
    excluded_items = {
        "PeriodicTask",  # Users won't use, but keeping for ease of internal use / debugging.
        "RequestColumn",  # Will add in as a separate section when there's more functionality.
        "start",  # Users won't use, but keeping for ease of internal use / debugging.
        "stop",  # Users won't use, but keeping for ease of internal use / debugging.
        "playground",  # Utility method - should add.
        "UDF",  # TODO: add to docs.
        "UserDefinedFunction",  # TODO: add to docs.
        "FunctionParameter",  # TODO: add to docs.
    }
    for item in all_exposed_items:
        if item in items and item in excluded_items:
            assert False, (
                f"Item {item} is both in items and excluded_items. Please update "
                f"the test_all_init_items_are_exposed test. You'll likely want to remove the item from "
                f"excluded_items."
            )
        if item in items or item in excluded_items:
            # Skip if docs exist, or we explicitly want to exclude it.
            continue
        missing_items.append(item)
    assert len(missing_items) == 0, f"Missing items: {missing_items}"
