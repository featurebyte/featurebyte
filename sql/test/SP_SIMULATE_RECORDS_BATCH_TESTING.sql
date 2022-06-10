create or replace procedure SP_SIMULATE_RECORDS_BATCH_TESTING(TABLE_NAME varchar, RUN_SECOND float, RUN_TS timestamp_tz, NUM_MINUTES float)
returns string
language javascript
as
$$
    var debug = "Debug"

    for (let i = NUM_MINUTES; i > 0; i--) {

        var run_ts = new Date(RUN_TS.getTime())
        run_ts.setSeconds(RUN_SECOND)
        run_ts.setMinutes(run_ts.getMinutes() - i)

        var sql = `
            call SP_SIMULATE_RECORD_GENERNATE_SCHEDULE('${TABLE_NAME}', ${RUN_SECOND}, '${run_ts.toISOString()}')
        `
        var result = snowflake.execute({sqlText: sql})
        result.next()
        debug = debug + " - SP_SIMULATE_RECORD_GENERNATE_SCHEDULE " + i + ": " + result.getColumnValue(1)
    }

    return debug
$$;
