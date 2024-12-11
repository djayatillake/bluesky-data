MODEL (
  name raw_http_sqlmesh.incremental_followers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (handle, actor)  ),
    audits (
      unique_combination_of_columns(columns := (handle, actor)),
    ),
);

SELECT
  CAST(did AS TEXT) AS did,
  CAST(handle AS TEXT) AS handle,
  CAST(display_name AS TEXT) AS display_name,
  CAST(avatar AS TEXT) AS avatar,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(description AS TEXT) AS description,
  CAST(indexed_at AS TIMESTAMP) AS indexed_at,
  CAST(actor AS TEXT) AS actor,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  CAST(associated__chat__allow_incoming AS TEXT) AS associated__chat__allow_incoming,
  CAST(associated__labeler AS BOOLEAN) AS associated__labeler,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  raw_http.followers
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
