import logging
import multiprocessing as mp
from multiprocessing import Queue, Process
from queue import Empty
import os
import pyarrow as pa
import pyarrow.json as pj
import pyarrow.parquet as pq
from datetime import datetime
from client import create_client

# Configuration
actor = os.getenv("bsky_actor")

# Create a global client instance
client = create_client()

def get_followers(actor: str):
    """Get all followers for an actor"""
    return client.get_followers(actor)

def get_follows(actor: str):
    """Get all accounts that an actor follows"""
    return client.get_follows(actor)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def format_progress(current, total):
    """Format progress string"""
    percent = (current / total) * 100
    return f"[{current}/{total} {percent:.1f}%]"

def fetch_actors():
    """Fetch list of actors to process"""
    # Set to store unique handles
    unique_handles = set()
    
    # Get followers
    for follower in get_followers(actor):
        if follower['handle'] != 'handle.invalid':
            unique_handles.add(follower['handle'])
    
    # Get follows
    for follow in get_follows(actor):
        if follow['handle'] != 'handle.invalid':
            unique_handles.add(follow['handle'])
    
    logger.info(f"Collected follows and followers for root actor: {actor}")
    logger.info(f"Found {len(unique_handles)} actors to process")
    
    # Add root actor
    unique_handles.add(actor)
    return list(unique_handles)

def collect_data(current_actor):
    """Collect data for a single actor and return it"""
    start_time = datetime.now()
    try:
        logger.debug(f"Collecting follows for {current_actor}")
        follows_data = list(get_follows(current_actor))
        
        logger.debug(f"Collecting followers for {current_actor}")
        followers_data = list(get_followers(current_actor))
        
        duration = (datetime.now() - start_time).total_seconds()
        return current_actor, True, (follows_data, followers_data), None, duration
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Error collecting data for {current_actor}: {str(e)}")
        return current_actor, False, None, str(e), duration

def worker(task_queue, result_queue, worker_id, total_actors):
    """Worker process to collect data"""
    while True:
        try:
            actor = task_queue.get()
            if actor is None:  # Poison pill
                logger.info(f"Worker {worker_id} received shutdown signal")
                break
                
            logger.info(f"Worker {worker_id} processing {actor}")
            result = collect_data(actor)
            result_queue.put(result)
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {str(e)}")
            break
    logger.info(f"Worker {worker_id} shutting down")

def ensure_pond_directories():
    """Ensure pond directories exist for storing parquet files"""
    os.makedirs("pond/follows", exist_ok=True)
    os.makedirs("pond/followers", exist_ok=True)

def save_results(result_queue, num_actors):
    """Save results to parquet files and track progress"""
    processed = 0
    successful = 0
    failed = 0
    total_duration = 0
    
    # Lists to store data
    follows_data = []
    followers_data = []
    follows_size = 0
    followers_size = 0
    follows_file_count = 0
    followers_file_count = 0
    
    # Ensure pond directories exist
    ensure_pond_directories()
    
    logger.info(f"Starting to collect results for {num_actors} actors")
    start_time = datetime.now()
    
    def write_follows_batch():
        nonlocal follows_data, follows_size, follows_file_count
        if follows_data:
            follows_file_count += 1
            filename = f"pond/follows/follows_{follows_file_count:04d}.parquet"
            # Convert JSON records directly to Arrow table and write to parquet
            table = pa.Table.from_pylist(follows_data)
            pq.write_table(table, filename)
            logger.info(f"Wrote follows batch {follows_file_count} ({len(follows_data)} records, {follows_size/1024/1024:.1f}MB)")
            follows_data = []
            follows_size = 0
            
    def write_followers_batch():
        nonlocal followers_data, followers_size, followers_file_count
        if followers_data:
            followers_file_count += 1
            filename = f"pond/followers/followers_{followers_file_count:04d}.parquet"
            # Convert JSON records directly to Arrow table and write to parquet
            table = pa.Table.from_pylist(followers_data)
            pq.write_table(table, filename)
            logger.info(f"Wrote followers batch {followers_file_count} ({len(followers_data)} records, {followers_size/1024/1024:.1f}MB)")
            followers_data = []
            followers_size = 0
    
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
                actor_follows, actor_followers = data
                
                # Estimate size increase for follows
                follows_size += sum(len(str(item)) for item in actor_follows)  # Rough estimate
                follows_data.extend(actor_follows)
                if follows_size >= 300 * 1024 * 1024:  # 100MB
                    write_follows_batch()
                
                # Estimate size increase for followers
                followers_size += sum(len(str(item)) for item in actor_followers)  # Rough estimate
                followers_data.extend(actor_followers)
                if followers_size >= 300 * 1024 * 1024:  # 100MB
                    write_followers_batch()
                
                successful += 1
                logger.info(f"{progress} Collected data for {actor_name} (took {duration:.1f}s, avg {avg_duration:.1f}s, ETA {eta_minutes:.1f}min)")
            else:
                failed += 1
                logger.error(f"{progress} Failed to process {actor_name}: {error}")
                
        except Empty:
            logger.error("Timeout waiting for results")
            break
        except Exception as e:
            logger.error(f"Error in save_results: {str(e)}")
            break
    
    # Write any remaining data
    write_follows_batch()
    write_followers_batch()
    
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
    for actor in actors:
        task_queue.put(actor)
    
    # Add poison pills for workers
    num_processes = min(99, num_actors)  # Don't create more processes than actors, tried 200 and it got too many requests errors from API
    for _ in range(num_processes):
        task_queue.put(None)
    
    # Start worker processes
    processes = []
    process_last_active = {}  # Track when each process last produced output
    for i in range(num_processes):
        p = Process(target=worker, args=(task_queue, result_queue, i+1, num_actors))
        p.daemon = True  # Make workers daemon processes so they exit when main process exits
        p.start()
        process_last_active[p.pid] = datetime.now()
        processes.append(p)
    
    # Collect results and save to parquet
    successful, failed = save_results(result_queue, num_actors)
    
    # Wait for processes to complete with timeout
    logger.info("Waiting for worker processes to complete...")
    
    # Give processes up to 1 minutes each to complete, but check for stuck processes
    MAX_INACTIVE_TIME = 60  # 1 minutes without any activity
    GRACEFUL_SHUTDOWN_TIME = 30  # 30 seconds for graceful shutdown
    
    for p in processes:
        try:
            # Check if process has been inactive for too long
            inactive_time = (datetime.now() - process_last_active[p.pid]).total_seconds()
            
            if inactive_time > MAX_INACTIVE_TIME:
                logger.warning(f"Process {p.pid} has been inactive for {inactive_time:.1f} seconds, terminating...")
                p.terminate()
                p.join(timeout=GRACEFUL_SHUTDOWN_TIME)
                if p.is_alive():
                    logger.error(f"Process {p.pid} could not be terminated, killing...")
                    os.kill(p.pid, 9)
            else:
                # Process is still active or recently active, give it time to complete
                p.join(timeout=MAX_INACTIVE_TIME)
                if p.is_alive():
                    logger.warning(f"Process {p.pid} did not complete within timeout, terminating...")
                    p.terminate()
                    p.join(timeout=GRACEFUL_SHUTDOWN_TIME)
                    if p.is_alive():
                        logger.error(f"Process {p.pid} could not be terminated, killing...")
                        os.kill(p.pid, 9)
        except Exception as e:
            logger.error(f"Error while joining process {p.pid}: {str(e)}")
    
    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Processing complete in {total_time/60:.1f} minutes:")
    logger.info(f"- Successful: {successful}")
    logger.info(f"- Failed: {failed}")
    logger.info(f"- Total: {num_actors}")
    logger.info(f"Data written to pond/follows/*.parquet and pond/followers/*.parquet")

if __name__ == '__main__':
    main()

# TODO: it would be good to find who the followers of people I follow are also following
# and add that data to the follows table, but it is likely to be a lot of data and a huge number of API calls