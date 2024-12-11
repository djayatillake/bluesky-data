MODEL (
  name raw_http_sqlmesh.jetstream,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ts
  ),
  allow_partials true,
  cron '@hourly',
);

SELECT 
    did, 
    kind, 
    make_timestamp(time_us) as ts, 
    commit.collection, 
    commit.record, 
    try_strptime(REPLACE(commit.record['createdAt'][1], '"', ''), '%Y-%m-%dT%H:%M:%S.%gZ') as createdAt, 
    commit.record['subject'], 
    commit.record['subject'][1].uri as uri, 
    'https://bsky.app/profile/' || 
    SPLIT_PART(SPLIT_PART(uri, '/app.bsky', 1), 'at://', 2) || 
    '/' || 
    SPLIT_PART(SPLIT_PART(uri, '/', 4), '.', -1) || 
    '/' || 
    SPLIT_PART(uri, '/', -1) as bsky_subject_url
FROM bluesky.jetstream.jetstream
WHERE ts BETWEEN @start_ts AND @end_ts