"""
Tests sql generation for parent features serving
"""

import json
import textwrap

from bson import ObjectId

from featurebyte.models.parent_serving import (
    EntityLookupInfo,
    EntityLookupStep,
    ParentServingPreparation,
)
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.node.schema import FeatureStoreDetails, TableDetails
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities
from tests.util.helper import assert_equal_with_expected_fixture


def test_construct_request_table_with_parent_entities(parent_serving_preparation):
    result = construct_request_table_with_parent_entities(
        "REQUEST_TABLE",
        request_table_columns=["a", "b"],
        join_steps=parent_serving_preparation.join_steps,
        feature_store_details=parent_serving_preparation.feature_store_details,
    )
    expected = textwrap.dedent(
        """
        SELECT
          REQ."a" AS "a",
          REQ."b" AS "b",
          REQ."COL_INT" AS "COL_INT"
        FROM (
          SELECT
            REQ."a",
            REQ."b",
            "T0"."COL_INT" AS "COL_INT"
          FROM "REQUEST_TABLE" AS REQ
          LEFT JOIN (
            SELECT
              "COL_TEXT",
              ANY_VALUE("COL_INT") AS "COL_INT"
            FROM (
              SELECT
                "col_text" AS "COL_TEXT",
                "col_int" AS "COL_INT"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."dimension_table"
              )
            )
            GROUP BY
              "COL_TEXT"
          ) AS T0
            ON REQ."COL_TEXT" = T0."COL_TEXT"
        ) AS REQ
        """
    ).strip()
    assert result.table_expr.sql(pretty=True) == expected
    assert result.parent_entity_columns == ["COL_INT"]
    assert result.new_request_table_name == "JOINED_PARENTS_REQUEST_TABLE"
    assert result.new_request_table_columns == ["a", "b", "COL_INT"]


def test_construct_request_table_with_parent_entities_table_details(parent_serving_preparation):
    """
    Test construct_request_table_with_parent_entities when the request table is a table in the
    warehouse
    """
    result = construct_request_table_with_parent_entities(
        None,
        request_table_columns=["a", "b"],
        join_steps=parent_serving_preparation.join_steps,
        feature_store_details=parent_serving_preparation.feature_store_details,
        request_table_details=TableDetails(
            database_name="my_db", schema_name="my_schema", table_name="my_table"
        ),
    )
    expected = textwrap.dedent(
        """
        SELECT
          REQ."a" AS "a",
          REQ."b" AS "b",
          REQ."COL_INT" AS "COL_INT"
        FROM (
          SELECT
            REQ."a",
            REQ."b",
            "T0"."COL_INT" AS "COL_INT"
          FROM "my_db"."my_schema"."my_table" AS REQ
          LEFT JOIN (
            SELECT
              "COL_TEXT",
              ANY_VALUE("COL_INT") AS "COL_INT"
            FROM (
              SELECT
                "col_text" AS "COL_TEXT",
                "col_int" AS "COL_INT"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."dimension_table"
              )
            )
            GROUP BY
              "COL_TEXT"
          ) AS T0
            ON REQ."COL_TEXT" = T0."COL_TEXT"
        ) AS REQ
        """
    ).strip()
    assert result.table_expr.sql(pretty=True) == expected
    assert result.parent_entity_columns == ["COL_INT"]
    assert result.new_request_table_name == "JOINED_PARENTS"
    assert result.new_request_table_columns == ["a", "b", "COL_INT"]


def test_construct_request_table_with_parent_entities_snapshots_table(update_fixtures):
    """
    Test construct_request_table_with_parent_entities with SnapshotsTable
    """
    # Create SnapshotsTable model with city -> district lookup
    snapshots_table_data = SnapshotsTableModel(
        name="city_district_snapshots",
        tabular_source={
            "feature_store_id": ObjectId(),
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "snapshots_table",
            },
        },
        columns_info=[
            {"name": "city", "dtype": "VARCHAR"},
            {"name": "district", "dtype": "VARCHAR"},
            {"name": "snapshot_date", "dtype": "VARCHAR"},
            {"name": "snapshot_id", "dtype": "INT"},
        ],
        snapshot_id_column="city",
        snapshot_datetime_column="snapshot_date",
        snapshot_datetime_schema={"format_string": "YYYY-MM-DD HH24:MI:SS"},
        time_interval={"unit": "DAY", "value": 1},
    )

    # Load feature store details from fixture
    with open("tests/fixtures/request_payloads/feature_store.json") as f:
        feature_store_details = FeatureStoreDetails(**json.load(f))

    # Create parent serving preparation with SnapshotsTable
    parent_serving_preparation = ParentServingPreparation(
        join_steps=[
            EntityLookupStep(
                id=ObjectId(),
                table=snapshots_table_data,
                parent=EntityLookupInfo(
                    key="district",
                    serving_name="DISTRICT",
                    entity_id=ObjectId(),
                ),
                child=EntityLookupInfo(
                    key="city",
                    serving_name="CITY",
                    entity_id=ObjectId(),
                ),
            )
        ],
        feature_store_details=feature_store_details,
    )

    result = construct_request_table_with_parent_entities(
        "REQUEST_TABLE",
        request_table_columns=["POINT_IN_TIME", "CITY"],
        join_steps=parent_serving_preparation.join_steps,
        feature_store_details=parent_serving_preparation.feature_store_details,
    )

    # Check SQL generation and basic result properties
    assert_equal_with_expected_fixture(
        result.table_expr.sql(pretty=True),
        "tests/fixtures/query_graph/test_parent_serving/snapshots_table_lookup.sql",
        update_fixtures,
    )
    assert result.parent_entity_columns == ["DISTRICT"]
    assert result.new_request_table_name == "JOINED_PARENTS_REQUEST_TABLE"
    assert result.new_request_table_columns == ["POINT_IN_TIME", "CITY", "DISTRICT"]
