CREATE OR REPLACE PROCEDURE SP_SIMULATE_RECORD_INSERT(TABLE_NAME varchar, RUN_SECOND float, SECONDS_STR varchar, RUN_TS timestamp_tz)
returns string
language javascript
as
$$
    /*
        Stored Procedure to insert simulated records
    */

    var debug = "Debug"

	var diff_seconds = SECONDS_STR.split(",")
	var cat_list = ["CAT_0", "CAT_1", "CAT_2", "CAT_3"]

	var values = []

	for (let i = 0; i < diff_seconds.length; i++) {
		event_ts = new Date(RUN_TS.getTime())
		event_ts.setSeconds(RUN_SECOND)
		event_ts.setMilliseconds(0)
		event_ts.setSeconds(event_ts.getSeconds() - diff_seconds[i])

		val = Math.floor(Math.random() * 10)
		cat = cat_list[Math.floor(Math.random() * cat_list.length)]
		values.push(`(1, '${event_ts.toISOString()}', '${cat}', ${val})`)
	}

	var sql = `
		INSERT INTO ${TABLE_NAME} 
		(ENTITY_ID, EVENT_TIMESTAMP, CATEGORY, VALUE) 
		VALUES ${values.join(",")}
	`
	snowflake.execute({sqlText: sql})
  
    return debug
$$;
