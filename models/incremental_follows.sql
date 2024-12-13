MODEL (
  name raw_http_sqlmesh.incremental_follows,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (handle, actor)
  ),
  audits (UNIQUE_COMBINATION_OF_COLUMNS(columns := (handle, actor)))
);

SELECT
  did::TEXT AS did,
  handle::TEXT AS handle,
  display_name::TEXT AS display_name,
  avatar::TEXT AS avatar,
  created_at::TIMESTAMP AS created_at,
  description::TEXT AS description,
  indexed_at::TIMESTAMP AS indexed_at,
  actor::TEXT AS actor,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  associated__chat__allow_incoming::TEXT AS associated__chat__allow_incoming,
  associated__labeler::BOOLEAN AS associated__labeler,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_load_time
FROM raw_http.follows
WHERE
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) BETWEEN @start_ds AND @end_ds
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY handle, actor ORDER BY _dlt_load_time DESC) = 1