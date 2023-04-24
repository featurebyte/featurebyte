"""
Doc layout test module
"""
import featurebyte
from featurebyte.common.documentation.documentation_layout import get_overall_layout


def test_all_init_methods_are_exposed():
    """
    Test that all methods in __init__ are exposed in the docs.
    """
    all_exposed_methods = featurebyte.__all__
    overall_layout = get_overall_layout()
    classes_and_properties = set()
    for item in overall_layout:
        classes_and_properties.add(item.menu_header[-1])  # Add the method/property.
        classes_and_properties.add(item.menu_header[0])  # Add top level nav item.
    missing_methods = []
    # excluded_methods are methods that are in __init__ but are not exposed in the docs.
    # Add to this list if you add a new method to __init__, but do not want to expose it in the docs for whatever
    # reason.
    excluded_methods = {
        "Configurations",
        "Series",
        "to_timedelta",
        "AccessTokenCredential",
        "Credential",
        "S3StorageCredential",
        "UsernamePasswordCredential",
        "DefaultVersionMode",
        "PeriodicTask",
        "start",
        "stop",
        "playground",
    }
    for method in all_exposed_methods:
        if method in classes_and_properties and method in excluded_methods:
            assert False, (
                f"Method {method} is both in classes_and_properties and excluded_methods. Please update "
                f"the test_all_init_methods_are_exposed test. You'll likely want to remove the method from "
                f"excluded_methods."
            )
        if method in classes_and_properties or method in excluded_methods:
            # Skip if docs exist, or we explicitly want to exclude it.
            continue
        missing_methods.append(method)
    assert len(missing_methods) == 0, f"Missing methods: {missing_methods}"
