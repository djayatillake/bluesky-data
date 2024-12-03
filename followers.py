import duckdb
from utils import (
    get_followers,
    get_follows,
    create_actor_field,
    pipeline_name,
    dataset_name,
    actor,
    pipeline,
)

table_name = "followers"

pipeline.run(get_followers(actor).add_map(create_actor_field(actor)),
             table_name=table_name,
             write_disposition="append",
             )
print(actor)   

con = duckdb.connect(database = pipeline_name + ".duckdb", read_only = False)
sql = "SELECT handle FROM " + dataset_name + "." + table_name + " WHERE actor = '" + actor + "'"
con.execute(sql)
actors = con.fetchall()

for index, actor in enumerate(actors):
    pipeline.run(get_followers(actor[0]).add_map(create_actor_field(actor[0])),
                 table_name=table_name,
                 write_disposition="append",
                 )
    pipeline.run(get_follows(actor[0]).add_map(create_actor_field(actor[0])),
                 table_name="follows",
                 write_disposition="append",
                 )
    print(actor[0], index)
