MODEL (
  name raw_http_sqlmesh.incremental__dlt_loads,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(load_id AS TEXT) AS load_id,
  CAST(schema_name AS TEXT) AS schema_name,
  CAST(status AS BIGINT) AS status,
  CAST(inserted_at AS TIMESTAMP) AS inserted_at,
  CAST(schema_version_hash AS TEXT) AS schema_version_hash,
  TO_TIMESTAMP(CAST(load_id AS DOUBLE)) as _dlt_load_time
FROM
  raw_http._dlt_loads
WHERE
  TO_TIMESTAMP(CAST(load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
