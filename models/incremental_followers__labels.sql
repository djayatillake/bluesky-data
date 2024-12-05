MODEL (
  name raw_http_sqlmesh.incremental_followers__labels,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(src AS TEXT) AS src,
  CAST(uri AS TEXT) AS uri,
  CAST(cid AS TEXT) AS cid,
  CAST(val AS TEXT) AS val,
  CAST(cts AS TIMESTAMP) AS cts,
  CAST(_dlt_parent_id AS TEXT) AS _dlt_parent_id,
  CAST(_dlt_list_idx AS BIGINT) AS _dlt_list_idx,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  CAST(ver AS BIGINT) AS ver,
  CAST(neg AS BOOLEAN) AS neg,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  raw_http.followers__labels
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
