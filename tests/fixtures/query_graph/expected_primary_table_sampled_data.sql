SELECT
  CAST(L."GroceryInvoiceGuid" AS VARCHAR) AS "GroceryInvoiceGuid",
  CAST(L."GroceryCustomerGuid" AS VARCHAR) AS "GroceryCustomerGuid",
  L."Timestamp" AS "Timestamp",
  L."Amount" AS "Amount",
  CAST(R."Gender" AS VARCHAR) AS "Gender"
FROM (
  SELECT
    "__FB_KEY_COL_0",
    "__FB_LAST_TS",
    "GroceryInvoiceGuid",
    "GroceryCustomerGuid",
    "Timestamp",
    "Amount"
  FROM (
    SELECT
      "__FB_KEY_COL_0",
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
      "GroceryInvoiceGuid",
      "GroceryCustomerGuid",
      "Timestamp",
      "Amount",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CAST("Timestamp" AS TIMESTAMP) AS TIMESTAMP) AS "__FB_TS_COL",
        "GroceryCustomerGuid" AS "__FB_KEY_COL_0",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        2 AS "__FB_TS_TIE_BREAKER_COL",
        "GroceryInvoiceGuid" AS "GroceryInvoiceGuid",
        "GroceryCustomerGuid" AS "GroceryCustomerGuid",
        "Timestamp" AS "Timestamp",
        "Amount" AS "Amount"
      FROM (
        SELECT
          "GroceryInvoiceGuid" AS "GroceryInvoiceGuid",
          "GroceryCustomerGuid" AS "GroceryCustomerGuid",
          "Timestamp" AS "Timestamp",
          "Amount" AS "Amount"
        FROM (
          SELECT
            RANDOM(1234) AS "prob"
          FROM (
            SELECT
            FROM "FEATUREBYTE_TESTING"."GROCERY"."GROCERYINVOICE"
          )
        )
        WHERE
          "prob" <= 0.015
        ORDER BY
          "prob"
        LIMIT 10
      )
      UNION ALL
      SELECT
        CAST(CAST("ValidFrom" AS TIMESTAMP) AS TIMESTAMP) AS "__FB_TS_COL",
        "GroceryCustomerGuid" AS "__FB_KEY_COL_0",
        "ValidFrom" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "GroceryInvoiceGuid",
        NULL AS "GroceryCustomerGuid",
        NULL AS "Timestamp",
        NULL AS "Amount"
      FROM (
        SELECT
          "RowID" AS "RowID",
          "GroceryCustomerGuid" AS "GroceryCustomerGuid",
          "ValidFrom" AS "ValidFrom",
          "Gender" AS "Gender"
        FROM "FEATUREBYTE_TESTING"."GROCERY"."GROCERYUSER"
        WHERE
          "ValidFrom" IS NOT NULL
      )
    )
  )
  WHERE
    "__FB_EFFECTIVE_TS_COL" IS NULL
) AS L
LEFT JOIN (
  SELECT
    FIRST("RowID") AS "RowID",
    "GroceryCustomerGuid",
    "ValidFrom",
    FIRST("Gender") AS "Gender"
  FROM (
    SELECT
      "RowID" AS "RowID",
      "GroceryCustomerGuid" AS "GroceryCustomerGuid",
      "ValidFrom" AS "ValidFrom",
      "Gender" AS "Gender"
    FROM "FEATUREBYTE_TESTING"."GROCERY"."GROCERYUSER"
    WHERE
      "ValidFrom" IS NOT NULL
  )
  GROUP BY
    "ValidFrom",
    "GroceryCustomerGuid"
) AS R
  ON L."__FB_LAST_TS" = R."ValidFrom" AND L."__FB_KEY_COL_0" = R."GroceryCustomerGuid"
