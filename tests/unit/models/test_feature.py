"""
Tests for Feature related models
"""
from datetime import datetime

import pytest

from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureListModel,
    FeatureModel,
    FeatureNameSpace,
    FeatureReadiness,
)


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature():
    """Fixture for a Feature dict"""
    return {
        "name": "sum_30m",
        "description": None,
        "var_type": "FLOAT",
        "lineage": ("groupby_1", "project_2"),
        "row_index_lineage": ("groupby_1",),
        "graph": {
            "edges": {
                "input_1": ["groupby_1"],
                "groupby_1": ["project_1"],
            },
            "backward_edges": {
                "groupby_1": ["input_1"],
                "project_1": ["groupby_1"],
            },
            "nodes": {
                "groupby_1": {
                    "name": "groupby_1",
                    "output_type": "frame",
                    "parameters": {
                        "agg_func": "sum",
                        "blind_spot": 600,
                        "frequency": 1800,
                        "keys": ["cust_id"],
                        "names": ["sum_30m"],
                        "parent": "col_float",
                        "tile_id": "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7",
                        "time_modulo_frequency": 300,
                        "timestamp": "event_timestamp",
                        "value_by": None,
                        "windows": ["30m"],
                    },
                    "type": "groupby",
                },
                "input_1": {
                    "name": "input_1",
                    "output_type": "frame",
                    "parameters": {
                        "columns": [
                            "col_int",
                            "col_float",
                            "col_char",
                            "col_text",
                            "col_binary",
                            "col_boolean",
                            "event_timestamp",
                            "created_at",
                            "cust_id",
                        ],
                        "database_source": {
                            "details": {
                                "account": "sf_account",
                                "database": "sf_database",
                                "sf_schema": "sf_schema",
                                "warehouse": "sf_warehouse",
                            },
                            "type": "snowflake",
                        },
                        "dbtable": {
                            "database_name": "sf_database",
                            "schema_name": "sf_schema",
                            "table_name": "sf_table",
                        },
                        "timestamp": "event_timestamp",
                    },
                    "type": "input",
                },
                "project_1": {
                    "name": "project_1",
                    "output_type": "series",
                    "parameters": {"columns": ["sum_30m"]},
                    "type": "project",
                },
            },
        },
        "node": {
            "name": "project_1",
            "parameters": {"columns": ["sum_30m"]},
            "type": "project",
            "output_type": "series",
        },
        "tabular_source": (
            {
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
                "type": "snowflake",
            },
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
        "readiness": None,
        "version": None,
        "created_at": None,
    }


@pytest.fixture(name="feature_list_model_dict")
def feature_list_model_dict_fixture():
    """Fixture for a FeatureList dict"""
    return {
        "name": "my_feature_list",
        "description": None,
        "features": [],
        "readiness": None,
        "status": None,
        "feature_list_version": "",
        "created_at": None,
    }


@pytest.fixture(name="feature_name_space_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    return {
        "name": "some_feature_name",
        "description": None,
        "versions": [],
        "readiness": FeatureReadiness.DRAFT,
        "created_at": datetime.now(),
        "default_version": "some_version",
        "default_version_mode": DefaultVersionMode.MANUAL,
    }


def test_feature_model(snowflake_event_view, feature_model_dict):
    """Test feature model serialize & deserialize"""
    snowflake_event_view.cust_id.as_entity("customer")
    feature_group = snowflake_event_view.groupby(by_keys="cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=["sum_30m"],
    )
    feature = feature_group["sum_30m"]
    assert feature.dict() == feature_model_dict
    feature_json = feature.json()
    feature_loaded = FeatureModel.parse_raw(feature_json)
    for key in feature_model_dict.keys():
        if key not in {"graph", "node"}:
            # feature_json uses pruned graph, feature uses global graph,
            # therefore the graph & node are different
            assert getattr(feature, key) == getattr(feature_loaded, key)


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    feature_list_dict = feature_list.dict()
    assert feature_list_dict == feature_list_model_dict


def test_feature_name_space(feature_name_space_dict):
    """Test feature name space model"""
    feature_name_space = FeatureNameSpace.parse_obj(feature_name_space_dict)
    feat_name_space_dict = feature_name_space.dict()
    assert feat_name_space_dict == feature_name_space_dict
