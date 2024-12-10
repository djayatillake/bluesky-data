MODEL (
  name raw_http_sqlmesh.dim_profiles,
  kind VIEW,
  references (
    raw_http_sqlmesh.incremental_followers,
    raw_http_sqlmesh.incremental_follows
  )
);

SELECT DISTINCT
  did,
  handle,
  display_name,
  avatar,
  description
FROM (
  SELECT did, handle, display_name, avatar, description
  FROM raw_http_sqlmesh.incremental_followers
  
  UNION ALL
  
  SELECT did, handle, display_name, avatar, description
  FROM raw_http_sqlmesh.incremental_follows
)
