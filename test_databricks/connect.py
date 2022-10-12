from databricks import sql as databricks_sql

connection = databricks_sql.connect(
    server_hostname="2085793316075774.4.gcp.databricks.com",
    http_path="sql/protocolv1/o/2085793316075774/1010-041133-p07dh5j6",
    access_token="dapi04004b8a8c85e210bf5b57783d8720cd",
    catalog="hive_metastore",
    schema="default",
)

sql = """
CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX(event_timestamp_str VARCHAR, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS INTEGER
  AS
  $$
      select index from (

        select

          (time_modulo_frequency_seconds - blind_spot_seconds) as offset,

          dateadd(second, -offset, event_timestamp_str::timestamp_ntz) as adjusted_ts,

          date_part(epoch_second, adjusted_ts) as period_in_seconds,

          floor(period_in_seconds / (frequency_minute*60)) as index
      )
  $$
"""

with connection.cursor() as cursor:
    cursor.execute(" show user functions ")

    # cursor.execute(sql_create_func)
    result = cursor.fetchall()

    for row in result:
        print(row)
