/* attach 'https://hive.buz.dev/bluesky/catalog' as jetstream;
attach 'bluesky.duckdb' as bluesky;
create schema if not exists bluesky.jetstream;
create table if not exists bluesky.jetstream.jetstream as select * from jetstream.jetstream;
insert into bluesky.jetstream.jetstream select * from jetstream.jetstream; */

attach 'https://hive.buz.dev/bluesky/catalog' as jetstream;
attach 'bluesky.duckdb' as bluesky;
insert into bluesky.jetstream.jetstream select * from jetstream.jetstream;