MODEL (
  name raw_http_sqlmesh.follows_of_follows,
  kind VIEW,
  references (raw_http_sqlmesh.base_actor_follows, raw_http_sqlmesh.incremental_follows)
);

WITH base_follows AS (
  /* Get all handles that the base actor follows */
  SELECT
    handle AS follow_handle
  FROM raw_http_sqlmesh.base_actor_follows
), follows_of_follows AS (
  /* Get all handles that are followed by the handles that base actor follows */
  SELECT DISTINCT
    f.did,
    f.handle,
    f.display_name
  FROM raw_http_sqlmesh.incremental_follows AS f
  INNER JOIN base_follows AS bf
    ON f.actor = bf.follow_handle
  WHERE
    NOT f.handle IN (
      /* Exclude handles that base actor already follows */
      SELECT
        follow_handle
      FROM base_follows
    )
)
/* Group and count to show popularity */
SELECT
  handle,
  did,
  display_name,
  COUNT(*) AS follower_count
FROM follows_of_follows
GROUP BY
  handle,
  did,
  display_name
ORDER BY
  follower_count DESC