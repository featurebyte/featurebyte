CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000000" AS
SELECT * FROM (
            select
                index,
                "col1", col2,
                current_timestamp() as created_at
            from (select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE)
        );

SELECT
  *
FROM "TILE_ID_1"
LIMIT 0;

ALTER TABLE tile_id_1 ADD COLUMN col1 FLOAT;


            merge into tile_id_1 a using __TEMP_TILE_TABLE_000000000000000000000000 b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."col1", b."col1")
                when matched then
                    update set a.created_at = current_timestamp(), a.col1 = b.col1
                when not matched then
                    insert (INDEX, "col1", col1, CREATED_AT)
                        values (b.INDEX, b."col1", b.col1, current_timestamp())
        ;

SELECT
  *
FROM "TILE_ID_2"
LIMIT 0;

ALTER TABLE tile_id_2 ADD COLUMN col2 FLOAT;


            merge into tile_id_2 a using __TEMP_TILE_TABLE_000000000000000000000000 b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."col1", b."col1")
                when matched then
                    update set a.created_at = current_timestamp(), a.col2 = b.col2
                when not matched then
                    insert (INDEX, "col1", col2, CREATED_AT)
                        values (b.INDEX, b."col1", b.col2, current_timestamp())
        ;
