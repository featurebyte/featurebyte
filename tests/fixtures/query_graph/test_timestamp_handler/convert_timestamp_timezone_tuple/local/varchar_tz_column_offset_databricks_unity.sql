FROM_UTC_TIMESTAMP(
  TO_TIMESTAMP(`zipped_timestamp_tuple`.timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''),
  `zipped_timestamp_tuple`.timezone
)
