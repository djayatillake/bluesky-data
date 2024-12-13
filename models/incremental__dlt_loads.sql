MODEL (
  name raw_http_sqlmesh.incremental__dlt_loads,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time
  )
);

SELECT
  load_id::TEXT AS load_id,
  schema_name::TEXT AS schema_name,
  status::BIGINT AS status,
  inserted_at::TIMESTAMP AS inserted_at,
  schema_version_hash::TEXT AS schema_version_hash,
  TO_TIMESTAMP(load_id::DOUBLE) AS _dlt_load_time
FROM raw_http._dlt_loads
WHERE
  TO_TIMESTAMP(load_id::DOUBLE) BETWEEN @start_ds AND @end_ds