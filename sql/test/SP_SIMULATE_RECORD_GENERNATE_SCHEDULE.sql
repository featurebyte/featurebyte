CREATE OR REPLACE PROCEDURE SP_SIMULATE_RECORD_GENERNATE_SCHEDULE(TABLE_NAME varchar, RUN_SECOND float, RUN_TS timestamp_tz)
returns string
language javascript
as
$$
    /*
        Stored Procedure to simulate record generation
    */

    var debug = "Debug"

    // wair for 5 seconds
    snowflake.execute({sqlText: `call system$wait(${RUN_SECOND})`})

    try {
        snowflake.execute({sqlText: `SELECT * FROM ${TABLE_NAME} LIMIT 1`})
    } catch (err)  {
        snowflake.execute({sqlText: `
        	CREATE TABLE ${TABLE_NAME} (
	        	ID int autoincrement,
	        	ENTITY_ID int, 
	        	EVENT_TIMESTAMP timestamp, 
	        	CATEGORY varchar,
	        	VALUE int,
                WH_AVAILABLE_AT timestamp,
	        	CREATED_AT timestamp default sysdate()
	        )
        `})
    }
	  
    job_minute_modulo = RUN_TS.getMinutes() % 10
    debug = debug + " - job_minute_modulo: " + job_minute_modulo

    var diff_seconds_str = ""
	if (job_minute_modulo === 1) {
    	// insert 3 records for those whose event_timestamp minute is 0. One of the records comes after 30 seconds and miss the WH job
    	diff_seconds_str = [55, 45, 35].join(',')
    	
    } else if (job_minute_modulo === 2) {
    	// insert 5 records. 4 of the records event_timestamp minute is 1
    	// and 1 late record whose event_timestamp minute is 0
    	diff_seconds_str = [85, 55, 45, 35, 25].join(',')

    } else if (job_minute_modulo === 4) {
    	// insert 8 records. 4 of the records event_timestamp minute is 2
    	// and the other 4 records event_timestamp is 3
    	diff_seconds_str = [
    		115, 105, 95, 85, 
    		55, 45, 35, 25
    	].join(',')
 
    } else if (job_minute_modulo === 9) {
    	// insert 20 records for those whose event_timestamp minute is 4 and 5, 6, 7 or 8
    	diff_seconds_str = [
    		295, 285, 275, 265,
    		235, 225, 215, 205,    	
    		175, 165, 155, 145,
    		115, 105, 95, 85, 
    		55, 45, 35, 25
    	].join(',')
    }
    debug = debug + " - diff_seconds_str: " + diff_seconds_str

    if (diff_seconds_str !== "") {
    	var result = snowflake.execute({sqlText: `call SP_SIMULATE_RECORD_INSERT('${TABLE_NAME}', ${RUN_SECOND}, '${diff_seconds_str}', '${RUN_TS.toISOString()}')`})
        result.next()
    	debug = debug + " - SP_SIMULATE_RECORD_INSERT: " + result.getColumnValue(1)
    }

    return debug
$$;
