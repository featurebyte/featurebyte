"""
Unit tests for timezone offset related functionality in API objects
"""
import textwrap

from featurebyte import FeatureJobSetting, MissingValueImputation
from featurebyte.api.feature import Feature
from tests.util.helper import check_sdk_code_generation


def test_protected_attributes__event_view(
    snowflake_event_table_with_tz_offset_column,
):
    """
    Test that protected attributes include timezone offset column
    """
    view = snowflake_event_table_with_tz_offset_column.get_view()
    assert view.protected_columns == {"tz_offset", "cust_id", "col_int", "event_timestamp"}


def test_protected_attributes__item_view(
    snowflake_item_table_with_timezone_offset_column,
):
    """
    Test protected attributes include timestamp timezone offset column
    """
    view = snowflake_item_table_with_timezone_offset_column.get_view(event_suffix="_event_table")
    assert view.protected_columns == {
        "cust_id_event_table",
        "tz_offset_event_table",
        "item_id_col",
        "event_id_col",
        "event_timestamp_event_table",
    }
    assert view.timestamp_timezone_offset_column == "tz_offset_event_table"


def test_datetime_property_extraction__event_timestamp(
    snowflake_event_table_with_tz_offset_constant, update_fixtures
):
    """
    Test extracting datetime property from event timestamp
    """
    view = snowflake_event_table_with_tz_offset_constant.get_view()
    timestamp_hour = view["event_timestamp"].dt.hour
    view["event_timestamp_hour"] = timestamp_hour

    # Check DT_EXTRACT node set up correctly
    assert timestamp_hour.node.parameters.dict() == {
        "property": "hour",
        "timezone_offset": "-05:30",
    }
    dt_extract_input_nodes = timestamp_hour.graph.backward_edges_map[timestamp_hour.node.name]
    assert len(dt_extract_input_nodes) == 1

    expected = textwrap.dedent(
        """
        SELECT
          "event_timestamp" AS "event_timestamp",
          "col_int" AS "col_int",
          "cust_id" AS "cust_id",
          "tz_offset" AS "tz_offset",
          EXTRACT(hour FROM DATEADD(second, F_TIMEZONE_OFFSET_TO_SECOND('-05:30'), "event_timestamp")) AS "event_timestamp_hour"
        FROM "sf_database"."sf_schema"."sf_table_no_tz"
        LIMIT 10
        """
    ).strip()
    assert view.preview_sql() == expected

    check_sdk_code_generation(
        view,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/event_view_with_tz_offset_constant.py",
        update_fixtures=update_fixtures,
        table_id=snowflake_event_table_with_tz_offset_constant.id,
    )


def test_datetime_property_extraction__event_timestamp_joined_view(
    snowflake_event_table_with_tz_offset_column, snowflake_dimension_table, update_fixtures
):
    """
    Test extracting datetime property from event timestamp lookup feature
    """
    view = snowflake_event_table_with_tz_offset_column.get_view()
    view = view.join(snowflake_dimension_table.get_view()[["col_int", "col_text"]])
    timestamp_hour = view["event_timestamp"].dt.hour
    view["event_timestamp_hour"] = timestamp_hour

    # Check DT_EXTRACT node set up correctly
    assert timestamp_hour.node.parameters.dict() == {"property": "hour", "timezone_offset": None}
    dt_extract_input_nodes = timestamp_hour.graph.backward_edges_map[timestamp_hour.node.name]
    assert len(dt_extract_input_nodes) == 2
    _, tz_offset_node = dt_extract_input_nodes
    assert view.graph.get_node_by_name(tz_offset_node).parameters.dict() == {
        "columns": ["tz_offset"]
    }

    expected = textwrap.dedent(
        """
        SELECT
          L."event_timestamp" AS "event_timestamp",
          L."col_int" AS "col_int",
          L."cust_id" AS "cust_id",
          L."tz_offset" AS "tz_offset",
          R."col_text" AS "col_text",
          EXTRACT(hour FROM DATEADD(second, F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset"), L."event_timestamp")) AS "event_timestamp_hour"
        FROM (
          SELECT
            "event_timestamp" AS "event_timestamp",
            "col_int" AS "col_int",
            "cust_id" AS "cust_id",
            "tz_offset" AS "tz_offset"
          FROM "sf_database"."sf_schema"."sf_table_no_tz"
        ) AS L
        LEFT JOIN (
          SELECT
            "col_int" AS "col_int",
            "col_text" AS "col_text"
          FROM "sf_database"."sf_schema"."dimension_table"
        ) AS R
          ON L."col_int" = R."col_int"
        LIMIT 10
        """
    ).strip()
    assert view.preview_sql() == expected

    check_sdk_code_generation(
        view,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/event_view_with_tz_offset_column.py",
        update_fixtures=update_fixtures,
        table_id=snowflake_event_table_with_tz_offset_column.id,
        dimension_table_id=snowflake_dimension_table.id,
    )


def test_datetime_property_extraction__manually_specified_timezone_offset(
    snowflake_event_table_with_tz_offset_constant, update_fixtures
):
    """
    Test extracting datetime property with manually specified timezone offset
    """
    view = snowflake_event_table_with_tz_offset_constant.get_view()
    timestamp_hour = view["event_timestamp"].dt.tz_offset("+08:00").hour
    view["event_timestamp_hour"] = timestamp_hour

    # Check DT_EXTRACT node set up correctly
    assert timestamp_hour.node.parameters.dict() == {
        "property": "hour",
        "timezone_offset": "+08:00",
    }
    dt_extract_input_nodes = timestamp_hour.graph.backward_edges_map[timestamp_hour.node.name]
    assert len(dt_extract_input_nodes) == 1

    check_sdk_code_generation(
        view,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/event_view_with_tz_offset_constant_manual.py",
        update_fixtures=update_fixtures,
        table_id=snowflake_event_table_with_tz_offset_constant.id,
    )


def test_datetime_property_extraction__manually_specified_timezone_offset_column(
    snowflake_event_table_with_tz_offset_constant, update_fixtures
):
    """
    Test extracting datetime property with manually specified timezone offset column
    """
    view = snowflake_event_table_with_tz_offset_constant.get_view()
    timestamp_hour = view["event_timestamp"].dt.tz_offset(view["tz_offset"]).hour
    view["event_timestamp_hour"] = timestamp_hour

    # Check DT_EXTRACT node set up correctly
    assert timestamp_hour.node.parameters.dict() == {"property": "hour", "timezone_offset": None}
    dt_extract_input_nodes = timestamp_hour.graph.backward_edges_map[timestamp_hour.node.name]
    assert len(dt_extract_input_nodes) == 2
    _, tz_offset_node = dt_extract_input_nodes
    assert view.graph.get_node_by_name(tz_offset_node).parameters.dict() == {
        "columns": ["tz_offset"]
    }

    check_sdk_code_generation(
        view,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/event_view_with_tz_offset_column_manual.py",
        update_fixtures=update_fixtures,
        table_id=snowflake_event_table_with_tz_offset_constant.id,
    )


def test_datetime_property_extraction__event_timestamp_in_item_view(
    snowflake_item_table_with_timezone_offset_column,
):
    """
    Test datetime property extraction from an ItemView with event timestamp timezone offset column
    """
    view = snowflake_item_table_with_timezone_offset_column.get_view(event_suffix="_event_table")
    timestamp_hour = view["event_timestamp_event_table"].dt.hour
    view["timestamp_hour"] = timestamp_hour

    # Check DT_EXTRACT node set up correctly
    assert timestamp_hour.node.parameters.dict() == {"property": "hour", "timezone_offset": None}
    dt_extract_input_nodes = timestamp_hour.graph.backward_edges_map[timestamp_hour.node.name]
    assert len(dt_extract_input_nodes) == 2
    _, tz_offset_node = dt_extract_input_nodes
    assert view.graph.get_node_by_name(tz_offset_node).parameters.dict() == {
        "columns": ["tz_offset_event_table"]
    }

    expected = textwrap.dedent(
        """
        SELECT
          L."event_id_col" AS "event_id_col",
          L."item_id_col" AS "item_id_col",
          L."item_type" AS "item_type",
          L."item_amount" AS "item_amount",
          CAST(L."created_at" AS STRING) AS "created_at",
          CAST(L."event_timestamp" AS STRING) AS "event_timestamp",
          R."event_timestamp" AS "event_timestamp_event_table",
          R."cust_id" AS "cust_id_event_table",
          R."tz_offset" AS "tz_offset_event_table",
          EXTRACT(hour FROM DATEADD(second, F_TIMEZONE_OFFSET_TO_SECOND(R."tz_offset"), R."event_timestamp")) AS "timestamp_hour"
        FROM (
          SELECT
            "event_id_col" AS "event_id_col",
            "item_id_col" AS "item_id_col",
            "item_type" AS "item_type",
            "item_amount" AS "item_amount",
            "created_at" AS "created_at",
            "event_timestamp" AS "event_timestamp"
          FROM "sf_database"."sf_schema"."items_table"
        ) AS L
        INNER JOIN (
          SELECT
            "event_timestamp" AS "event_timestamp",
            "col_int" AS "col_int",
            "cust_id" AS "cust_id",
            "tz_offset" AS "tz_offset"
          FROM "sf_database"."sf_schema"."sf_table_no_tz"
        ) AS R
          ON L."event_id_col" = R."col_int"
        LIMIT 10
        """
    ).strip()
    assert view.preview_sql() == expected


def test_datetime_property_extraction__event_timestamp_in_item_view_joined_scd_view(
    snowflake_item_table_with_timezone_offset_column, snowflake_scd_view
):
    """
    Test datetime property extraction from an ItemView with event timestamp timezone offset column
    """
    view = snowflake_item_table_with_timezone_offset_column.get_view(event_suffix="_event_table")
    view = view.join(snowflake_scd_view, on="cust_id_event_table", rsuffix="_joined")
    view["customer_age"] = (
        view["event_timestamp_event_table"].dt.year - view["date_of_birth_joined"].dt.year
    )
    cond = view["event_timestamp_event_table"].dt.month < view["date_of_birth_joined"].dt.month
    view["customer_age"][cond] = view["customer_age"][cond] - 1
    expected = textwrap.dedent(
        """
        SELECT
          L."event_id_col" AS "event_id_col",
          L."item_id_col" AS "item_id_col",
          L."item_type" AS "item_type",
          L."item_amount" AS "item_amount",
          CAST(L."created_at" AS STRING) AS "created_at",
          CAST(L."event_timestamp" AS STRING) AS "event_timestamp",
          L."event_timestamp_event_table" AS "event_timestamp_event_table",
          L."cust_id_event_table" AS "cust_id_event_table",
          L."tz_offset_event_table" AS "tz_offset_event_table",
          R."col_float" AS "col_float_joined",
          R."col_binary" AS "col_binary_joined",
          R."col_boolean" AS "col_boolean_joined",
          R."date_of_birth" AS "date_of_birth_joined",
          CAST(R."created_at" AS STRING) AS "created_at_joined",
          R."cust_id" AS "cust_id_joined",
          CASE
            WHEN (
              EXTRACT(month FROM DATEADD(
                second,
                F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
                L."event_timestamp_event_table"
              )) < EXTRACT(month FROM R."date_of_birth")
            )
            THEN (
              (
                EXTRACT(year FROM DATEADD(
                  second,
                  F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
                  L."event_timestamp_event_table"
                )) - EXTRACT(year FROM R."date_of_birth")
              ) - 1
            )
            ELSE (
              EXTRACT(year FROM DATEADD(
                second,
                F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
                L."event_timestamp_event_table"
              )) - EXTRACT(year FROM R."date_of_birth")
            )
          END AS "customer_age"
        FROM (
          SELECT
            "__FB_KEY_COL_0",
            "__FB_LAST_TS",
            "event_id_col",
            "item_id_col",
            "item_type",
            "item_amount",
            "created_at",
            "event_timestamp",
            "event_timestamp_event_table",
            "cust_id_event_table",
            "tz_offset_event_table"
          FROM (
            SELECT
              "__FB_KEY_COL_0",
              LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
              "event_id_col",
              "item_id_col",
              "item_type",
              "item_amount",
              "created_at",
              "event_timestamp",
              "event_timestamp_event_table",
              "cust_id_event_table",
              "tz_offset_event_table",
              "__FB_EFFECTIVE_TS_COL"
            FROM (
              SELECT
                CAST(CONVERT_TIMEZONE('UTC', "event_timestamp_event_table") AS TIMESTAMP) AS "__FB_TS_COL",
                "cust_id_event_table" AS "__FB_KEY_COL_0",
                NULL AS "__FB_EFFECTIVE_TS_COL",
                2 AS "__FB_TS_TIE_BREAKER_COL",
                "event_id_col" AS "event_id_col",
                "item_id_col" AS "item_id_col",
                "item_type" AS "item_type",
                "item_amount" AS "item_amount",
                "created_at" AS "created_at",
                "event_timestamp" AS "event_timestamp",
                "event_timestamp_event_table" AS "event_timestamp_event_table",
                "cust_id_event_table" AS "cust_id_event_table",
                "tz_offset_event_table" AS "tz_offset_event_table"
              FROM (
                SELECT
                  L."event_id_col" AS "event_id_col",
                  L."item_id_col" AS "item_id_col",
                  L."item_type" AS "item_type",
                  L."item_amount" AS "item_amount",
                  L."created_at" AS "created_at",
                  L."event_timestamp" AS "event_timestamp",
                  R."event_timestamp" AS "event_timestamp_event_table",
                  R."cust_id" AS "cust_id_event_table",
                  R."tz_offset" AS "tz_offset_event_table"
                FROM (
                  SELECT
                    "event_id_col" AS "event_id_col",
                    "item_id_col" AS "item_id_col",
                    "item_type" AS "item_type",
                    "item_amount" AS "item_amount",
                    "created_at" AS "created_at",
                    "event_timestamp" AS "event_timestamp"
                  FROM "sf_database"."sf_schema"."items_table"
                ) AS L
                INNER JOIN (
                  SELECT
                    "event_timestamp" AS "event_timestamp",
                    "col_int" AS "col_int",
                    "cust_id" AS "cust_id",
                    "tz_offset" AS "tz_offset"
                  FROM "sf_database"."sf_schema"."sf_table_no_tz"
                ) AS R
                  ON L."event_id_col" = R."col_int"
              )
              UNION ALL
              SELECT
                CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
                "col_text" AS "__FB_KEY_COL_0",
                "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
                1 AS "__FB_TS_TIE_BREAKER_COL",
                NULL AS "event_id_col",
                NULL AS "item_id_col",
                NULL AS "item_type",
                NULL AS "item_amount",
                NULL AS "created_at",
                NULL AS "event_timestamp",
                NULL AS "event_timestamp_event_table",
                NULL AS "cust_id_event_table",
                NULL AS "tz_offset_event_table"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "effective_timestamp" AS "effective_timestamp",
                  "end_timestamp" AS "end_timestamp",
                  "date_of_birth" AS "date_of_birth",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."scd_table"
              )
            )
          )
          WHERE
            "__FB_EFFECTIVE_TS_COL" IS NULL
        ) AS L
        LEFT JOIN (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
        ) AS R
          ON L."__FB_LAST_TS" = R."effective_timestamp" AND L."__FB_KEY_COL_0" = R."col_text"
        LIMIT 10
        """
    ).strip()
    assert view.preview_sql() == expected


def test_feature_using_timezone_offset_with_cleaning_operations(
    snowflake_event_table_with_tz_offset_column, update_fixtures
):
    """
    Test creating a feature derived from event timestamp column where the timezone offset column
    requires cleaning operations
    """
    cleaning_ops = [MissingValueImputation(imputed_value="+00:00")]
    snowflake_event_table_with_tz_offset_column["tz_offset"].update_critical_data_info(cleaning_ops)

    view = snowflake_event_table_with_tz_offset_column.get_view()
    view["timestamp_hour"] = view["event_timestamp"].dt.hour

    feature = view.groupby("cust_id", category="timestamp_hour").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["timestamp_hour_counts_7d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )["timestamp_hour_counts_7d"]
    feature.save()

    # Check generated code for the saved feature (cleaning operations on the offset column should be
    # preserved)
    feature_loaded = Feature.get(feature.name)
    check_sdk_code_generation(
        feature_loaded,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_with_cleaned_tz_offset_column.py",
        update_fixtures=update_fixtures,
        table_id=snowflake_event_table_with_tz_offset_column.id,
    )
