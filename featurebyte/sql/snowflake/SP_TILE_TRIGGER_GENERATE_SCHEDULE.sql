CREATE OR REPLACE PROCEDURE SP_TILE_TRIGGER_GENERATE_SCHEDULE(
    SHELL_TASK_NAME varchar,
    WAREHOUSE varchar,
    FEATURE_NAME varchar,
    TIME_MODULO_FREQUENCY_SECONDS float,
    BLIND_SPOT_SECONDS float,
    FREQUENCY_MINUTE float,
    OFFLINE_PERIOD_MINUTE float,
    SQL varchar,
    TILE_START_DATE_COLUMN varchar,
    TILE_LAST_START_DATE_COLUMN varchar,
    TILE_START_DATE_PLACEHOLDER varchar,
    TILE_END_DATE_PLACEHOLDER varchar,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TYPE varchar,
    MONITOR_PERIODS float
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

    task_name = "TILE_TASK_" + TYPE + "_" + FEATURE_NAME
    // create and start new scheduled Task with Intervaled Schedule
    var sql = `
        CREATE OR REPLACE TASK ${task_name}
        WAREHOUSE = '${WAREHOUSE}'
        SCHEDULE = '${task_interval} MINUTE'
        AS
            CALL SP_TILE_GENERATE_SCHEDULE('${FEATURE_NAME}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, ${OFFLINE_PERIOD_MINUTE}, '${SQL}', '${TILE_START_DATE_COLUMN}', '${TILE_LAST_START_DATE_COLUMN}', '${TILE_START_DATE_PLACEHOLDER}', '${TILE_END_DATE_PLACEHOLDER}', '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', '${TYPE}', ${MONITOR_PERIODS}, to_varchar(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'))
    `
    snowflake.execute({sqlText: sql})

    if (SHELL_TASK_NAME != null) {
        snowflake.execute({sqlText: `ALTER TASK IF EXISTS ${task_name} RESUME`})
    }

    // remove the temporary task that trigger this stored procedure
    snowflake.execute({sqlText: `DROP TASK IF EXISTS ${SHELL_TASK_NAME}`})

    return debug
$$;
