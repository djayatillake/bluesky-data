MODEL (
  name raw_http_sqlmesh.jetstream,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ts
  ),
  allow_partials TRUE,
  cron '@hourly'
);

SELECT
  did,
  kind,
  MAKE_TIMESTAMP(time_us) AS ts,
  commit.collection,
  commit.record,
  TRY_STRPTIME(REPLACE(commit.record['createdAt'][1], '"', ''), '%Y-%m-%dT%H:%M:%S.%gZ') AS createdAt,
  commit.record['subject'],
  commit.record['subject'][1].uri AS uri,
  'https://bsky.app/profile/' || SPLIT_PART(SPLIT_PART(uri, '/app.bsky', 1), 'at://', 2) || '/' || SPLIT_PART(SPLIT_PART(uri, '/', 4), '.', -1) || '/' || SPLIT_PART(uri, '/', -1) AS bsky_subject_url
FROM bluesky.jetstream.jetstream
WHERE
  ts BETWEEN @start_ts AND @end_ts