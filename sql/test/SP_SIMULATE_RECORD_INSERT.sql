CREATE OR REPLACE PROCEDURE SP_SIMULATE_RECORD_INSERT(TABLE_NAME varchar, RUN_SECOND float, SECONDS_STR varchar, RUN_TS timestamp_tz)
returns string
language javascript
as
$$
    /*
        Stored Procedure to insert simulated records
    */

    var debug = "Debug"

	// insert 3 records for those whose event_timestamp minute is 0
	var diff_seconds = SECONDS_STR.split(",")
	var cat_list = ["CAT_0", "CAT_1", "CAT_2", "CAT_3"]

	var avail_ts = new Date(RUN_TS.getTime())
	avail_ts.setSeconds(RUN_SECOND)

	for (let i = 0; i < diff_seconds.length; i++) {
		event_ts = new Date(RUN_TS.getTime())
		event_ts.setSeconds(RUN_SECOND)
		event_ts.setMilliseconds(0)
		event_ts.setSeconds(event_ts.getSeconds() - diff_seconds[i])

		val = Math.floor(Math.random() * 10)
		cat = cat_list[Math.floor(Math.random() * cat_list.length)]

		snowflake.execute({sqlText: `
			INSERT INTO ${TABLE_NAME} 
			(ENTITY_ID, EVENT_TIMESTAMP, CATEGORY, VALUE, WH_AVAILABLE_AT) 
			VALUES (1, '${event_ts.toISOString()}', '${cat}', ${val}, '${avail_ts.toISOString()}')
		`})
	}

  
    return debug
$$;
