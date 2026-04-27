"""Redis queue manager for streaming data ingestion.
"""

import json
import time
from typing import Any, List, Optional, Dict, Union, Sequence

import redis
from redis.exceptions import RedisError

from .config import streaming_settings


class QueueManager:
    """Manages a Redis queue for streaming data."""

    def __init__(self, queue_name: Optional[str] = None):
        """
        Initialize the queue manager.

        Args:
            queue_name: Name of the Redis list to use as queue. If None, uses the default from settings.
        """
        self.queue_name = queue_name or streaming_settings.queue_name
        self._redis_client = None

    @property
    def redis_client(self) -> redis.Redis:
        """Lazy-loaded Redis client."""
        if self._redis_client is None:
            try:
                self._redis_client = redis.Redis(
                    host=streaming_settings.redis_host,
                    port=streaming_settings.redis_port,
                    db=streaming_settings.redis_db,
                    password=streaming_settings.redis_password,
                    decode_responses=False,  # We want bytes for flexibility
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )
                # Test connection
                self._redis_client.ping()
            except RedisError as e:
                raise ConnectionError(f"Failed to connect to Redis: {e}") from e
        return self._redis_client

    def push(self, item: Union[bytes, str, Dict, List]) -> int:
        """
        Push an item to the queue.

        Args:
            item: Item to push. If dict or list, will be JSON-encoded.

        Returns:
            Length of the queue after push.
        """
        if isinstance(item, (dict, list)):
            item = json.dumps(item).encode('utf-8')
        elif isinstance(item, str):
            item = item.encode('utf-8')
        # item is already bytes at this point

        return self.redis_client.lpush(self.queue_name, item)  # type: ignore

    def push_batch(self, items: Sequence[Union[bytes, str, Dict, List]]) -> int:
        """
        Push a batch of items to the queue.

        Args:
            items: List of items to push.

        Returns:
            Length of the queue after push.
        """
        if not items:
            return self.size()

        # Convert items to bytes
        encoded_items = []
        for item in items:
            if isinstance(item, (dict, list)):
                encoded_items.append(json.dumps(item).encode('utf-8'))
            elif isinstance(item, str):
                encoded_items.append(item.encode('utf-8'))
            else:
                encoded_items.append(item)  # assume bytes

        # Use pipeline for efficiency
        pipe = self.redis_client.pipeline()
        for item in encoded_items:
            pipe.lpush(self.queue_name, item)
        results = pipe.execute()
        return results[-1]  # Last result is the queue length after all pushes

    def pop(self, timeout: int = 0) -> Optional[bytes]:
        """
        Pop an item from the queue.

        Args:
            timeout: Timeout in seconds. If 0, returns immediately. If >0, blocks for up to timeout seconds.

        Returns:
            The popped item as bytes, or None if queue is empty (or timeout reached).
        """
        if timeout > 0:
            # Blocking pop
            result = self.redis_client.brpop(self.queue_name, timeout=timeout)
            if result is None:
                return None
            # brpop returns (list_name, value)
            return result[1]  # type: ignore
        else:
            return self.redis_client.rpop(self.queue_name)  # type: ignore

    def pop_batch(self, count: int, timeout: int = 0) -> List[bytes]:
        """
        Pop multiple items from the queue.

        Args:
            count: Maximum number of items to pop.
            timeout: Timeout in seconds for blocking pop. If 0, non-blocking.

        Returns:
            List of popped items (as bytes).
        """
        if count <= 0:
            return []

        items = []
        if timeout > 0:
            # We'll do a blocking pop for the first item, then non-blocking for the rest
            first_item = self.pop(timeout)
            if first_item is None:
                return []
            items.append(first_item)
            count -= 1

        # Pop remaining items non-blocking
        for _ in range(count):
            item = self.pop(timeout=0)
            if item is None:
                break
            items.append(item)

        return items

    def size(self) -> int:
        """Get the current queue size."""
        return self.redis_client.llen(self.queue_name)  # type: ignore

    def clear(self) -> None:
        """Clear the queue."""
        self.redis_client.delete(self.queue_name)

    def get_items(self, start: int = 0, end: int = -1) -> List[bytes]:
        """
        Get a range of items from the queue without removing them.

        Args:
            start: Start index (0-based).
            end: End index (inclusive, -1 for last).

        Returns:
            List of items in the specified range.
        """
        return self.redis_client.lrange(self.queue_name, start, end)  # type: ignore

    def health_check(self) -> bool:
        """Check if Redis is reachable and the queue is operational."""
        try:
            self.redis_client.ping()
            return True
        except RedisError:
            return False

    def wait_for_items(self, min_items: int = 1, timeout: int = 30, check_interval: float = 0.1) -> bool:
        """
        Wait until the queue has at least min_items.

        Args:
            min_items: Minimum number of items required.
            timeout: Maximum time to wait in seconds.
            check_interval: Time between checks in seconds.

        Returns:
            True if condition met, False if timeout.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.size() >= min_items:
                return True
            time.sleep(check_interval)
        return False


# Singleton instance for convenience
queue_manager = QueueManager()
