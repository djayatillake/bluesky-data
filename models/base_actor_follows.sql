MODEL (
  name raw_http_sqlmesh.base_actor_follows,
  kind EMBEDDED,
  references (
    raw_http_sqlmesh.incremental_follows
  )
);

SELECT DISTINCT
  did,
  handle,
  display_name
FROM raw_http_sqlmesh.incremental_follows
WHERE
  actor = @bsky_actor