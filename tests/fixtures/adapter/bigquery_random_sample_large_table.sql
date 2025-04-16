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
  `prob` <= 1.499999850000015e-05
LIMIT 100
