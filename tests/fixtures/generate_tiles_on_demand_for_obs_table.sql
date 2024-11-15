CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000000" AS
SELECT * FROM (
            select
                index,
                "col1", col2,
                current_timestamp() as created_at
            from (select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE)
        );
