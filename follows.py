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

table_name = "follows"

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
    pipeline.run(get_follows(actor[0]).add_map(create_actor_field(actor[0])),
                 table_name=table_name,
                 write_disposition="append",
                 )
    pipeline.run(get_followers(actor[0]).add_map(create_actor_field(actor[0])),
                 table_name="followers",
                 write_disposition="append",
                 )
    print(actor[0], index)

# TODO: it would be good to find who the followers of people I follow are also following
# and add that data to the follows table, but it is likely to be a lot of data and a huge number of API calls
# so we need to be probably take a cut of people who are followed a lot by the followers of my follows