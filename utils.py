import dlt
import os
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

# Shared constants
pipeline_name = "bluesky_data"
dataset_name = "raw_http"
actor = os.environ.get("bsky_actor")

# Shared client
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

@dlt.resource
def get_follows(actor: str):
    for page in bluesky_client.paginate(
        "app.bsky.graph.getFollows",
        params={
            "actor": actor,
            "limit": 100,
        },
    ):
        yield page

def create_actor_field(actor_str):
    def actor_field(data):
        data["actor"] = actor_str
        return data
    return actor_field 

pipeline = dlt.pipeline(
    pipeline_name=pipeline_name,
    destination="duckdb",
    dataset_name=dataset_name,
)