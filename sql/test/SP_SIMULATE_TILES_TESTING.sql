create or replace procedure SP_SIMULATE_TILES_TESTING(END_TS varchar, NUM_RECORDS float)
returns string
language javascript
as
$$
    var debug = "Debug"
    //var end_ts = new Date(Date.parse('2022-05-26 00:03:00 UTC'))
    var end_ts = new Date(Date.parse(END_TS + ' UTC'))
    const a_list = ['detail', 'view', 'purchase']

    for (let i = 0; i < NUM_RECORDS; i++) {
        var ts = new Date(end_ts.getTime())
        ts.setMinutes(ts.getMinutes() - 5)
        var action = a_list[Math.floor(Math.random() * 3)]
        var value = Math.floor(Math.random() * 20)
        var cust_id = Math.floor(Math.random() * 5)

        var sql = `
            insert into tile_testing (CUST_ID, PRODUCT_ACTION, TILE_START_TS, VALUE)
            VALUES (${cust_id}, '${action}', '${ts.toISOString()}', ${value})
        `
        snowflake.execute(
        {
            sqlText: sql
        })
        end_ts = ts
    }

    return debug
$$;
