from atproto import Client, models
import logging
from datetime import datetime
from typing import Iterator, Dict, Any, Optional

# Configure logging - silence all HTTP-related logs
for logger_name in ['atproto', 'urllib3', 'requests', 'httpx', 'httpcore']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)
    logging.getLogger(logger_name).propagate = False

logger = logging.getLogger(__name__)

class BlueskyClient:
    def __init__(self):
        # Use the public API endpoint
        self.client = Client(base_url="https://api.bsky.app")
    
    def get_followers(self, actor: str, limit: int = 100) -> Iterator[Dict[str, Any]]:
        """Get all followers for an actor using pagination"""
        cursor = None
        total_fetched = 0
        
        while True:
            try:
                # Build params dict for the API call
                params = {
                    "actor": actor,
                    "limit": limit
                }
                if cursor:
                    params["cursor"] = cursor
                
                # Use the official client to get followers
                response = self.client.app.bsky.graph.get_followers(params)
                
                followers = response.followers
                total_fetched += len(followers)
                logger.debug(f"Fetched {len(followers)} followers for {actor} (total: {total_fetched})")
                
                for follower in followers:
                    # Just add actor and timestamp to raw record
                    record = follower.model_dump()
                    record["actor"] = actor
                    record["indexed_at"] = datetime.utcnow().isoformat()
                    yield record
                
                # Get cursor for next page
                cursor = response.cursor
                if not cursor:
                    logger.debug(f"No more followers to fetch for {actor}")
                    break
                    
            except Exception as e:
                logger.error(f"Error getting followers for {actor}: {str(e)}")
                break
    
    def get_follows(self, actor: str, limit: int = 100) -> Iterator[Dict[str, Any]]:
        """Get all accounts that an actor follows using pagination"""
        cursor = None
        total_fetched = 0
        
        while True:
            try:
                # Build params dict for the API call
                params = {
                    "actor": actor,
                    "limit": limit
                }
                if cursor:
                    params["cursor"] = cursor
                
                # Use the official client to get follows
                response = self.client.app.bsky.graph.get_follows(params)
                
                follows = response.follows
                total_fetched += len(follows)
                logger.debug(f"Fetched {len(follows)} follows for {actor} (total: {total_fetched})")
                
                for follow in follows:
                    # Just add actor and timestamp to raw record
                    record = follow.model_dump()
                    record["actor"] = actor
                    record["indexed_at"] = datetime.utcnow().isoformat()
                    yield record
                
                # Get cursor for next page
                cursor = response.cursor
                if not cursor:
                    logger.debug(f"No more follows to fetch for {actor}")
                    break
                    
            except Exception as e:
                logger.error(f"Error getting follows for {actor}: {str(e)}")
                break

    def close(self):
        """Close the session"""
        pass  # No need to close the session as atproto client does not have a session

def create_client() -> BlueskyClient:
    """Create a new Bluesky client instance"""
    return BlueskyClient()
