SELECT
  `a`,
  `b`
FROM (
  SELECT
    RAND() AS `prob`,
    `a`,
    `b`
  FROM (
    SELECT
      a,
      b
    FROM table1
  )
)
WHERE
  `prob` <= 0.15000000000000002
LIMIT 100
