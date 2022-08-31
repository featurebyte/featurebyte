"""
Common test fixtures used across unit tests in tests/unit/core/accessor directory
"""
import textwrap

import pytest


@pytest.fixture(name="expression_sql_template")
def expression_sql_template_fixture():
    """SQL template used to construct the expected sql code"""
    template_sql = """
    SELECT
      {expression}
    FROM (
        SELECT
          "CUST_ID" AS "CUST_ID",
          "PRODUCT_ACTION" AS "PRODUCT_ACTION",
          "VALUE" AS "VALUE",
          "MASK" AS "MASK",
          "TIMESTAMP" AS "TIMESTAMP",
          "PROMOTION_START_DATE" AS "PROMOTION_START_DATE"
        FROM "db"."public"."transaction"
    )
    LIMIT 10
    """
    return textwrap.dedent(template_sql).strip()
