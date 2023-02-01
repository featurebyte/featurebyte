CREATE OR REPLACE PROCEDURE SP_TILE_TRIGGER_GENERATE_SCHEDULE(
    SHELL_TASK_NAME VARCHAR,
    WAREHOUSE VARCHAR,
    TILE_ID VARCHAR,
    AGGREGATION_ID VARCHAR,
    TIME_MODULO_FREQUENCY_SECONDS FLOAT,
    BLIND_SPOT_SECONDS FLOAT,
    FREQUENCY_MINUTE FLOAT,
    OFFLINE_PERIOD_MINUTE FLOAT,
    SQL VARCHAR,
    TILE_START_DATE_COLUMN VARCHAR,
    TILE_LAST_START_DATE_COLUMN VARCHAR,
    TILE_START_DATE_PLACEHOLDER VARCHAR,
    TILE_END_DATE_PLACEHOLDER VARCHAR,
    ENTITY_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_TYPES VARCHAR,
    TYPE VARCHAR,
    MONITOR_PERIODS FLOAT
)
returns string
language javascript
as
$$
    /*
        Stored Procedure to create scheduled Task for Tile generation and monitoring.
        This stored procedure will be run only once to create and start new Scheduled Task with Intervaled Schedule
    */

    var debug = "Debug"

    // derive and sleep cron_residue_seconds
    cron_residue_seconds = TIME_MODULO_FREQUENCY_SECONDS % 60
    snowflake.execute(
        {
            sqlText: `call system$wait(${cron_residue_seconds})`
        }
    )
    debug = debug + " - cron_residue_seconds: " + cron_residue_seconds

    var task_interval = FREQUENCY_MINUTE
    if (TYPE.toUpperCase() === "OFFLINE") {
        // offline schedule
        task_interval = OFFLINE_PERIOD_MINUTE
    }

    var tile_id = TILE_ID.toUpperCase()
    var task_name = "TILE_TASK_" + TYPE + "_" + AGGREGATION_ID

    // create and start new scheduled Task with Intervaled Schedule
    var sql = `
        CREATE OR REPLACE TASK ${task_name}
        WAREHOUSE = '${WAREHOUSE}'
        SCHEDULE = '${task_interval} MINUTE'
        AS
            CALL SP_TILE_GENERATE_SCHEDULE(
              '${tile_id}',
              '${AGGREGATION_ID}',
              ${TIME_MODULO_FREQUENCY_SECONDS},
              ${BLIND_SPOT_SECONDS},
              ${FREQUENCY_MINUTE},
              ${OFFLINE_PERIOD_MINUTE},
              '${SQL}',
              '${TILE_START_DATE_COLUMN}',
              '${TILE_LAST_START_DATE_COLUMN}',
              '${TILE_START_DATE_PLACEHOLDER}',
              '${TILE_END_DATE_PLACEHOLDER}',
              '${ENTITY_COLUMN_NAMES}',
              '${VALUE_COLUMN_NAMES}',
              '${VALUE_COLUMN_TYPES}',
              '${TYPE}',
              ${MONITOR_PERIODS},
              to_varchar(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"')
            )
    `
    snowflake.execute({sqlText: sql})

    if (SHELL_TASK_NAME != null) {
        snowflake.execute({sqlText: `ALTER TASK IF EXISTS ${task_name} RESUME`})
    }

    // remove the temporary task that trigger this stored procedure
    snowflake.execute({sqlText: `DROP TASK IF EXISTS ${SHELL_TASK_NAME}`})

    return debug
$$;
