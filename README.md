# bluesky-data
A project to try out SQLMesh using Bluesky data

Using dlt and jetstream to ingest batch and streaming Bluesky data, respectively.

The project aims to ingest data for an individual Bluesky account's surrounding graph regarding follows and followers.
Then, this data is used to find other new accounts to follow.

The individual base actor's account is assumed to be in an environment variable called bsky_actor, which is then used to populate a sqlmesh variable in config.yaml

follows_dlt.py can be run to ingest batch data for follows and followers.

follows.py is a version that doesn't use dlt or duckdb and outputs in parquet format to a /pond folder.

The dlt version of the pipeline creates bluesky.duckdb, a database which the sqlmesh project uses.

The SQLMesh project then builds models of different kinds to make the data ingested useful.

