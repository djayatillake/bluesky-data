import duckdb
import logging
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info(f"Starting data collection for root actor: {actor}")
pipeline.run(get_followers(actor).add_map(create_actor_field(actor)),
             table_name=table_name,
             write_disposition="append",
             )
logger.info(f"Collected follows for root actor: {actor}")

con = duckdb.connect(database = pipeline_name + ".duckdb", read_only = False)
sql = "SELECT handle FROM " + dataset_name + "." + table_name + " WHERE actor = '" + actor + "' and actor != 'handle.invalid'"
con.execute(sql)
actors = con.fetchall()
logger.info(f"Found {len(actors)} actors to process")

for index, actor in enumerate(actors):
    current_actor = actor[0]
    logger.info(f"Processing actor {index + 1}/{len(actors)}: {current_actor}")
    
    logger.debug(f"Collecting follows for {current_actor}")
    pipeline.run(get_follows(current_actor).add_map(create_actor_field(current_actor)),
                 table_name=table_name,
                 write_disposition="append",
                 )
    
    logger.debug(f"Collecting followers for {current_actor}")
    pipeline.run(get_followers(current_actor).add_map(create_actor_field(current_actor)),
                 table_name="followers",
                 write_disposition="append",
                 )
    logger.info(f"Completed processing {current_actor}")

# TODO: it would be good to find who the followers of people I follow are also following
# and add that data to the follows table, but it is likely to be a lot of data and a huge number of API calls
# so we need to be probably take a cut of people who are followed a lot by the followers of my follows