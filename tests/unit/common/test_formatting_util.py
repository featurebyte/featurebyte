"""
Test utility functions for formatting data in Jupyter notebooks
"""
from featurebyte.common.formatting_util import CodeStr, InfoDict


def test_codestr_formatting():
    """
    Test CodeStr formatting
    """
    code = CodeStr("import featurebyte")
    assert str(code) == "import featurebyte"
    assert code._repr_html_() == (
        '<div style="margin:30px; padding: 20px; border:1px solid #aaa">'
        '<div class="highlight"><pre style="line-height: 125%;"><span></span>'
        '<span style="color: #008000; font-weight: bold">import</span> '
        '<span style="color: #0000FF; font-weight: bold">featurebyte</span>\n'
        "</pre></div>\n</div>"
    )


def test_feature_list_info_dict_formatting(update_fixtures):
    """
    Test InfoDict formatting for feature list
    """
    feature_list_info = {
        "name": "Small List",
        "created_at": "2023-05-03T14:30:54.217000",
        "updated_at": "2023-05-13T06:55:12.110000",
        "entities": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery - playground (spark)",
            }
        ],
        "primary_entity": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery - playground (spark)",
            }
        ],
        "tables": [
            {
                "name": "GROCERYPRODUCT",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
            {
                "name": "INVOICEITEMS",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
            {
                "name": "GROCERYINVOICE",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
        ],
        "default_version_mode": "AUTO",
        "version_count": 1,
        "catalog_name": "Grocery - playground (spark)",
        "dtype_distribution": [
            {"dtype": "OBJECT", "count": 1},
            {"dtype": "FLOAT", "count": 8},
            {"dtype": "VARCHAR", "count": 1},
        ],
        "status": "PUBLIC_DRAFT",
        "feature_count": 10,
        "version": {"this": "V230503", "default": "V230503"},
        "production_ready_fraction": {"this": 1.0, "default": 1.0},
        "versions_info": None,
        "deployed": False,
        "class_name": "FeatureList",
    }

    feature_list_info_html = InfoDict(feature_list_info)._repr_html_()
    feature_list_info_fixture_path = "tests/fixtures/feature_list_info.html"
    if update_fixtures:
        with open(feature_list_info_fixture_path, "w") as file_obj:
            file_obj.write(feature_list_info_html)
    else:
        # check report
        with open(feature_list_info_fixture_path, "r") as file_obj:
            expected_html = file_obj.read()
        assert feature_list_info_html == expected_html


def test_feature_info_dict_formatting(update_fixtures):
    """
    Test InfoDict formatting for feature
    """
    feature_info = {
        "name": "CustomerSpendingStability_7d28d",
        "created_at": "2023-05-16T01:35:13.757000",
        "updated_at": None,
        "entities": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery V31 - playground (spark)",
            }
        ],
        "primary_entity": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery V31 - playground (spark)",
            }
        ],
        "tables": [
            {
                "name": "INVOICEITEMS",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery V31 - playground (spark)",
            },
            {
                "name": "GROCERYINVOICE",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery V31 - playground (spark)",
            },
        ],
        "default_version_mode": "AUTO",
        "version_count": 1,
        "catalog_name": "Grocery V31 - playground (spark)",
        "dtype": "FLOAT",
        "primary_table": [
            {
                "name": "INVOICEITEMS",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery V31 - playground (spark)",
            }
        ],
        "default_feature_id": "6462dd287b12204e7e1c2ba6",
        "version": {"this": "V230516", "default": "V230516"},
        "readiness": {"this": "DRAFT", "default": "DRAFT"},
        "table_feature_job_setting": {
            "this": [
                {
                    "table_name": "GROCERYINVOICE",
                    "feature_job_setting": {
                        "blind_spot": "3600s",
                        "frequency": "3600s",
                        "time_modulo_frequency": "60s",
                    },
                }
            ],
            "default": [
                {
                    "table_name": "GROCERYINVOICE",
                    "feature_job_setting": {
                        "blind_spot": "3600s",
                        "frequency": "3600s",
                        "time_modulo_frequency": "60s",
                    },
                }
            ],
        },
        "table_cleaning_operation": {"this": [], "default": []},
        "versions_info": None,
        "metadata": {
            "input_columns": {
                "Input0": {
                    "data": "INVOICEITEMS",
                    "column_name": "GroceryProductGuid",
                    "semantic": None,
                },
                "Input1": {"data": "INVOICEITEMS", "column_name": "TotalCost", "semantic": None},
            },
            "derived_columns": {},
            "aggregations": {
                "F0": {
                    "name": "CustomerSpending_7d",
                    "column": "Input1",
                    "function": "sum",
                    "keys": ["GroceryCustomerGuid"],
                    "window": "7d",
                    "category": "GroceryProductGuid",
                    "filter": False,
                },
                "F1": {
                    "name": "CustomerSpending_28d",
                    "column": "Input1",
                    "function": "sum",
                    "keys": ["GroceryCustomerGuid"],
                    "window": "28d",
                    "category": "GroceryProductGuid",
                    "filter": False,
                },
            },
            "post_aggregation": {
                "name": "CustomerSpendingStability_7d28d",
                "inputs": ["F0", "F1"],
                "transforms": ["cosine_similarity"],
            },
        },
        "class_name": "Feature",
    }

    feature_info_html = InfoDict(feature_info)._repr_html_()
    feature_info_fixture_path = "tests/fixtures/feature_info.html"
    if update_fixtures:
        with open(feature_info_fixture_path, "w") as file_obj:
            file_obj.write(feature_info_html)
    else:
        # check report
        with open(feature_info_fixture_path, "r") as file_obj:
            expected_html = file_obj.read()
        assert feature_info_html == expected_html
