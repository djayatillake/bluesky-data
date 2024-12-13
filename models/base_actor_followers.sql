MODEL (
  name raw_http_sqlmesh.base_actor_followers,
  kind EMBEDDED,
  references (
    raw_http_sqlmesh.incremental_followers
  )
);

SELECT DISTINCT
  did,
  handle,
  display_name
FROM raw_http_sqlmesh.incremental_followers
WHERE
  actor = @bsky_actor