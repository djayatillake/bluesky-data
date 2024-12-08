import duckdb
import logging
import multiprocessing as mp
from multiprocessing import Queue, Process
from queue import Empty
from datetime import datetime
import dlt
import os
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

# Shared constants
pipeline_name = "bluesky"
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_progress(current, total):
    percentage = (current / total) * 100
    return f"[{current}/{total} - {percentage:.1f}%]"

def fetch_actors():
    logger.info(f"Starting data collection for root actor: {actor}")
    pipeline.run(get_followers(actor).add_map(create_actor_field(actor)),
                table_name="followers",
                write_disposition="append",
                )
    pipeline.run(get_follows(actor).add_map(create_actor_field(actor)),
                table_name="follows",
                write_disposition="append",
                )
    logger.info(f"Collected follows and followers for root actor: {actor}")

    con = duckdb.connect(database=pipeline_name + ".duckdb", read_only=False)
    sql = f"""
    SELECT handle FROM {dataset_name}.followers WHERE actor = '{actor}' and actor != 'handle.invalid'
    UNION
    SELECT handle FROM {dataset_name}.follows WHERE actor = '{actor}' and actor != 'handle.invalid'
    """
    con.execute(sql)
    actors = con.fetchall()
    con.close()
    logger.info(f"Found {len(actors)} actors to process")
    return actors

def collect_data(current_actor):
    """Collect data for a single actor and return it"""
    start_time = datetime.now()
    try:
        logger.debug(f"Collecting follows for {current_actor}")
        follows_data = list(get_follows(current_actor).add_map(create_actor_field(current_actor)))
        
        logger.debug(f"Collecting followers for {current_actor}")
        followers_data = list(get_followers(current_actor).add_map(create_actor_field(current_actor)))
        
        duration = (datetime.now() - start_time).total_seconds()
        return current_actor, True, (follows_data, followers_data), None, duration
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Error collecting data for {current_actor}: {str(e)}")
        return current_actor, False, None, str(e), duration

def worker(task_queue, result_queue, worker_id, total_actors):
    """Worker process to collect data"""
    processed = 0
    while True:
        try:
            actor_tuple = task_queue.get(timeout=1)
            if actor_tuple is None:  # Poison pill
                break
            
            current_actor = actor_tuple[0]
            processed += 1
            logger.info(f"Worker {worker_id} {format_progress(processed, total_actors//5)}: Processing {current_actor}")
            result = collect_data(current_actor)
            result_queue.put(result)
            
        except Empty:
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {str(e)}")
            break

def save_results(result_queue, num_actors):
    """Save results to database from a single process"""
    processed = 0
    successful = 0
    failed = 0
    total_duration = 0
    
    logger.info(f"Starting to save results for {num_actors} actors")
    start_time = datetime.now()
    
    while processed < num_actors:
        try:
            actor_name, success, data, error, duration = result_queue.get(timeout=60)
            processed += 1
            total_duration += duration
            avg_duration = total_duration / processed
            
            progress = format_progress(processed, num_actors)
            eta_seconds = (num_actors - processed) * avg_duration
            eta_minutes = eta_seconds / 60
            
            if success:
                follows_data, followers_data = data
                try:
                    # Save follows
                    pipeline.run(
                        follows_data,
                        table_name="follows",
                        write_disposition="append",
                    )
                    # Save followers
                    pipeline.run(
                        followers_data,
                        table_name="followers",
                        write_disposition="append",
                    )
                    successful += 1
                    logger.info(f"{progress} Saved data for {actor_name} (took {duration:.1f}s, avg {avg_duration:.1f}s, ETA {eta_minutes:.1f}min)")
                except Exception as e:
                    failed += 1
                    logger.error(f"{progress} Error saving data for {actor_name}: {str(e)}")
            else:
                failed += 1
                logger.error(f"{progress} Failed to process {actor_name}: {error}")
                
        except Empty:
            logger.error("Timeout waiting for results")
            break
        except Exception as e:
            logger.error(f"Error in save_results: {str(e)}")
            break
    
    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Total processing time: {total_time/60:.1f} minutes")
    return successful, failed

def main():
    actors = fetch_actors()
    num_actors = len(actors)
    
    if num_actors == 0:
        logger.info("No actors to process")
        return
    
    logger.info(f"Starting processing of {num_actors} actors")
    start_time = datetime.now()
    
    # Create queues for tasks and results
    task_queue = Queue()
    result_queue = Queue()
    
    # Add tasks to queue
    for actor_tuple in actors:
        task_queue.put(actor_tuple)
    
    # Add poison pills for workers
    num_processes = min(50, num_actors)  # Don't create more processes than actors
    for _ in range(num_processes):
        task_queue.put(None)
    
    # Start worker processes
    processes = []
    for i in range(num_processes):
        p = Process(target=worker, args=(task_queue, result_queue, i+1, num_actors))
        p.start()
        processes.append(p)
    
    # Save results in the main process
    successful, failed = save_results(result_queue, num_actors)
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Processing complete in {total_time/60:.1f} minutes:")
    logger.info(f"- Successful: {successful}")
    logger.info(f"- Failed: {failed}")
    logger.info(f"- Total: {num_actors}")
    if failed > 0:
        logger.warning(f"Failed to process {failed} actors")

if __name__ == '__main__':
    main()

# TODO: it would be good to find who the followers of people I follow are also following
# and add that data to the follows table, but it is likely to be a lot of data and a huge number of API calls