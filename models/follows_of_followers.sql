MODEL (
  name raw_http_sqlmesh.follows_of_followers,
  kind VIEW,
  references (raw_http_sqlmesh.base_actor_followers, raw_http_sqlmesh.incremental_follows)
);

WITH base_followers AS (
  /* Get all handles that follow the base actor */
  SELECT
    handle AS follower_handle
  FROM raw_http_sqlmesh.base_actor_followers
), follows_of_followers AS (
  /* Get all handles that are followed by the handles that follow base actor */
  SELECT DISTINCT
    f.did,
    f.handle,
    f.display_name
  FROM raw_http_sqlmesh.incremental_follows AS f
  INNER JOIN base_followers AS bf
    ON f.actor = bf.follower_handle
  WHERE
    NOT f.handle IN (
      /* Exclude handles that base actor already follows */
      SELECT
        handle
      FROM raw_http_sqlmesh.base_actor_follows
    )
)
/* Group and count to show popularity */
SELECT
  handle,
  did,
  display_name,
  COUNT(*) AS follower_count
FROM follows_of_followers
GROUP BY
  handle,
  did,
  display_name
ORDER BY
  follower_count DESC