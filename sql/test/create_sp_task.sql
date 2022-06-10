CREATE OR REPLACE TASK TEMP_SCHEDULED_TASK
  WAREHOUSE = FB_TILE_WH
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
    call SP_SIMULATE_RECORD_GENERNATE_SCHEDULE('TEMP_SCHEDULED', 5, to_varchar(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'));
    

CREATE OR REPLACE TASK TEMP_SCHEDULED_FEATURE_JOB_25_TASK
  WAREHOUSE = FB_TILE_WH
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
    call FEATUREBYTE.SP_TILE_GENERATE_SCHEDULE('TEMP_SCHEDULED_25', 15, 25, 1, 1440, '
        select entity_id, F_INDEX_TO_TIMESTAMP(index, 15, 25, 1) as tile_start_ts, count(*) as value
        from (
            select 
                entity_id,            
                F_TIMESTAMP_TO_INDEX(event_timestamp, 15, 25, 1) as index,
                value
            from FB_TEST.TEMP_SCHEDULED
            where event_timestamp < FB_END_TS
            and event_timestamp >= FB_START_TS
        )
        where tile_start_ts < FB_END_TS
        and tile_start_ts >= FB_START_TS
        group by entity_id, tile_start_ts
    ', 
            'entity_id, tile_start_ts, value', 
            'online', 10, null);

CREATE OR REPLACE TASK TEMP_SCHEDULED_FEATURE_JOB_40_TASK
  WAREHOUSE = FB_TILE_WH
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
    call FEATUREBYTE.SP_TILE_GENERATE_SCHEDULE('TEMP_SCHEDULED_40', 15, 40, 1, 1440, '
        select entity_id, F_INDEX_TO_TIMESTAMP(index, 15, 40, 1) as tile_start_ts, count(*) as value
        from (
            select 
                entity_id,            
                F_TIMESTAMP_TO_INDEX(event_timestamp, 15, 40, 1) as index,
                value
            from FB_TEST.TEMP_SCHEDULED
            where event_timestamp < FB_END_TS
            and event_timestamp >= FB_START_TS
        )
        where tile_start_ts < FB_END_TS
        and tile_start_ts >= FB_START_TS
        group by entity_id, tile_start_ts
    ', 
            'entity_id, tile_start_ts, value', 
            'online', 10, null);
            
CREATE OR REPLACE TASK TEMP_SCHEDULED_FEATURE_JOB_100_TASK
  WAREHOUSE = FB_TILE_WH
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
    call FEATUREBYTE.SP_TILE_GENERATE_SCHEDULE('TEMP_SCHEDULED_100', 15, 100, 1, 1440, '
        select entity_id, F_INDEX_TO_TIMESTAMP(index, 15, 100, 1) as tile_start_ts, count(*) as value
        from (
            select 
                entity_id,            
                F_TIMESTAMP_TO_INDEX(event_timestamp, 15, 100, 1) as index,
                value
            from FB_TEST.TEMP_SCHEDULED
            where event_timestamp < FB_END_TS
            and event_timestamp >= FB_START_TS
        )
        where tile_start_ts < FB_END_TS
        and tile_start_ts >= FB_START_TS
        group by entity_id, tile_start_ts
    ', 
            'entity_id, tile_start_ts, value', 
            'online', 10, null);

CREATE OR REPLACE TASK TEMP_SCHEDULED_FEATURE_JOB_160_TASK
  WAREHOUSE = FB_TILE_WH
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
    call FEATUREBYTE.SP_TILE_GENERATE_SCHEDULE('TEMP_SCHEDULED_160', 15, 160, 1, 1440, '
        select entity_id, F_INDEX_TO_TIMESTAMP(index, 15, 160, 1) as tile_start_ts, count(*) as value
        from (
            select 
                entity_id,            
                F_TIMESTAMP_TO_INDEX(event_timestamp, 15, 160, 1) as index,
                value
            from FB_TEST.TEMP_SCHEDULED
            where event_timestamp < FB_END_TS
            and event_timestamp >= FB_START_TS
        )
        where tile_start_ts < FB_END_TS
        and tile_start_ts >= FB_START_TS
        group by entity_id, tile_start_ts
    ', 
            'entity_id, tile_start_ts, value', 
            'online', 10, null);


ALTER TASK TEMP_SCHEDULED_TASK RESUME;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_25_TASK RESUME;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_40_TASK RESUME;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_100_TASK RESUME;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_160_TASK RESUME;


ALTER TASK TEMP_SCHEDULED_TASK SUSPEND;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_25_TASK SUSPEND;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_40_TASK SUSPEND;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_100_TASK SUSPEND;

ALTER TASK TEMP_SCHEDULED_FEATURE_JOB_160_TASK SUSPEND;
