CREATE OR REPLACE PROCEDURE SP_TILE_ONLINE_LOOKBACK_PERIOD(TABLE_NAME varchar, TILE_END varchar, OFFLINE_PERIOD_MINUTE float, LOOKBACK_MINUTE float, TIME_MODULO_FREQUENCY_SECONDS float, BLIND_SPOT_SECONDS float, FREQUENCY_MINUTE float)
returns float
language javascript
as
$$
    /*
        Stored Procedure to calculate appropriate lookback period for ONLINE Tile computation

        1. minimum lookback period is 30 minutes (less than OFFLINE_PERIOD_MINUTE which is default to 1440 minutes)
        2. result_lookback_period = MAX(lookback, TILE_END_TS - last_tile_ts)
    */
    
    var result_lookback_period = 0

    var min_diff_sql = `
        select
            '${TILE_END}'::timestamp_ntz as tile_end_ts,
            F_INDEX_TO_TIMESTAMP(index, ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE})::timestamp_ntz as tile_start_ts,
            timediff(minute, tile_start_ts, tile_end_ts)
        from feature1_tile
        where tile_start_ts > dateadd(minute, -${OFFLINE_PERIOD_MINUTE}, tile_end_ts::timestamp_ntz)
        order by tile_start_ts desc
        limit 1
    `
    var result = snowflake.execute({sqlText: min_diff_sql})
    if (result.getRowCount() > 0) {
        result.next()
        result_lookback_period = result.getColumnValue(3)
    }
    
    
    if (LOOKBACK_MINUTE < 30) {
        LOOKBACK_MINUTE = 30
    }
    
    if (LOOKBACK_MINUTE > result_lookback_period) {
        result_lookback_period = LOOKBACK_MINUTE
    }
    
    return result_lookback_period
$$;
