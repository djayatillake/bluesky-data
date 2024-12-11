MODEL (
  name raw_http_sqlmesh.followers_of_follows,
  kind VIEW,
  references (
    raw_http_sqlmesh.base_actor_follows,
    raw_http_sqlmesh.incremental_followers
  )
);

WITH base_follows AS (
  -- Get all handles that the base actor follows
  SELECT handle as follow_handle
  FROM raw_http_sqlmesh.base_actor_follows
),
followers_of_follows AS (
  -- Get all handles that follow the handles that base actor follows
  SELECT DISTINCT 
    f.did,
    f.handle,
    f.display_name
  FROM raw_http_sqlmesh.incremental_followers f
  INNER JOIN base_follows bf
    ON bf.follow_handle = f.actor
  WHERE f.handle NOT IN (
    -- Exclude handles that base actor already follows
    SELECT follow_handle FROM base_follows
  )
)
-- Group and count to show popularity
SELECT 
  handle,
  did,
  display_name,
  COUNT(*) as follower_count
FROM followers_of_follows
GROUP BY 
  handle,
  did,
  display_name
ORDER BY follower_count DESC