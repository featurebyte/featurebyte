CREATE OR REPLACE PROCEDURE SP_TILE_TRIGGER_GENERATE_SCHEDULE(SHELL_TASK_NAME varchar, WAREHOUSE varchar, FEATURE_NAME varchar, TIME_MODULO_FREQUENCY_SECONDS float, BLIND_SPOT_SECONDS float, FREQUENCY_MINUTE float, OFFLINE_PERIOD_MINUTE float, SQL varchar, COLUMN_NAMES varchar, TYPE varchar, MONITOR_PERIODS float)
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

    // create and start new scheduled Task with Intervaled Schedule
    var sql = `
        CREATE OR REPLACE TASK TILE_TASK_${TYPE}_${FEATURE_NAME}
        WAREHOUSE = '${WAREHOUSE}'
        SCHEDULE = '${FREQUENCY_MINUTE} MINUTE'
        AS
            CALL SP_TILE_GENERATE_SCHEDULE('${FEATURE_NAME}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, ${OFFLINE_PERIOD_MINUTE}, '${SQL}', '${COLUMN_NAMES}', '${TYPE}', ${MONITOR_PERIODS}, to_varchar(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'))
    `
    snowflake.execute({sqlText: sql})

    if (SHELL_TASK_NAME != null) {
        snowflake.execute({sqlText: `ALTER TASK IF EXISTS SP_TILE_GENERATE_SCHEDULE_TASK_${FEATURE_NAME} RESUME`})
    }

    // remove the temporary task that trigger this stored procedure
    snowflake.execute({sqlText: `DROP TASK IF EXISTS ${SHELL_TASK_NAME}`})

    return debug
$$;
