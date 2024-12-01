import dlt
import os
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

pipeline_name = "bluesky_data"
dataset_name = "raw_http"
table_name = "followers"

bluesky_client = RESTClient(
    base_url="https://public.api.bsky.app/xrpc/",
    paginator=JSONResponseCursorPaginator(cursor_path="cursor", cursor_param="cursor"),
)

@dlt.resource
def get_followers(actor: str):
    for page in bluesky_client.paginate(
        "app.bsky.graph.getFollowers",
        params={
            "actor": actor,
            "limit": 100,
        },
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name=pipeline_name,
    destination="duckdb",
    dataset_name=dataset_name,
)

actor = os.environ.get("bsky_actor")
def actor_field(data):
    data["actor"] = actor
    return data
load_info = pipeline.run(get_followers(actor).add_map(actor_field),
                         table_name=table_name,
                         write_disposition="replace",
                         )
print(load_info)

import duckdb
# to use a database file (shared between processes)
con = duckdb.connect(database = pipeline_name + ".duckdb", read_only = False)
sql = "SELECT handle FROM " + dataset_name + "." + table_name
con.execute(sql)
actors = con.fetchall()

for index, actor in enumerate(actors):
    def actor_field(data):
        data["actor"] = actor[0]
        return data
    load_info = pipeline.run(get_followers(actor[0]).add_map(actor_field),
                             table_name=table_name,
                             write_disposition="append",
                             )
    print(actor[0], index)
