FROM_UTC_TIMESTAMP(
  TO_TIMESTAMP(
    GET_JSON_OBJECT(`zipped_timestamp_tuple`, '$.timestamp'),
    'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
  ),
  GET_JSON_OBJECT(`zipped_timestamp_tuple`, '$.timezone')
)
