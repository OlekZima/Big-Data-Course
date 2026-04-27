"""
File watcher for automatic data injection.

This module monitors a directory for new Parquet files and pushes file paths to a queue for processing.
"""

import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional, Callable, List, Union, Any

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from .config import streaming_settings
from .queue_manager import QueueManager, queue_manager

logger = logging.getLogger(__name__)


class ParquetFileHandler(FileSystemEventHandler):
    """Handler for Parquet file events."""

    def __init__(self, queue_callback: Callable[[Path], None], pattern: str = "*.parquet"):
        """
        Initialize the handler.

        Args:
            queue_callback: Function to call when a new Parquet file is detected.
            pattern: File pattern to watch (default: *.parquet).
        """
        self.queue_callback = queue_callback
        self.pattern = pattern

    def on_created(self, event: Union[DirCreatedEvent, FileCreatedEvent]):
        """Called when a file is created."""
        if not event.is_directory:
            file_path = Path(str(event.src_path))
            if file_path.match(self.pattern):
                logger.info(f"Detected new Parquet file: {file_path}")
                self.queue_callback(file_path)


class FileWatcher:
    """Watches a directory for new Parquet files and pushes them to a queue."""

    def __init__(
        self,
        watch_path: Optional[Path] = None,
        pattern: Optional[str] = None,
        poll_interval: Optional[float] = None,
        queue_manager_instance: Optional[QueueManager] = None,
    ):
        """
        Initialize the file watcher.

        Args:
            watch_path: Directory to watch. Defaults to settings.
            pattern: File pattern to watch. Defaults to settings.
            poll_interval: Polling interval for the observer. Defaults to settings.
            queue_manager: Queue manager to use for pushing file paths. Defaults to the singleton.
        """
        self.watch_path = watch_path or streaming_settings.watch_path
        self.pattern = pattern or streaming_settings.watch_pattern
        self.poll_interval = poll_interval or streaming_settings.poll_interval
        self.queue_manager = queue_manager_instance or queue_manager

        self.observer: Optional[Any] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Ensure the watch path exists
        self.watch_path.mkdir(parents=True, exist_ok=True)

    def _push_file_to_queue(self, file_path: Path) -> None:
        """
        Push a file path to the queue.

        Args:
            file_path: Path to the Parquet file.
        """
        # Create a message with file information
        message = {
            "type": "file",
            "path": str(file_path.absolute()),
            "timestamp": time.time(),
            "size": file_path.stat().st_size if file_path.exists() else 0,
        }
        # Push the message as JSON
        self.queue_manager.push(message)
        logger.info(f"Pushed file to queue: {file_path}")

    def _process_existing_files(self) -> None:
        """Process existing Parquet files in the watch directory."""
        existing_files = list(self.watch_path.rglob(self.pattern))
        logger.info(f"Found {len(existing_files)} existing Parquet files.")
        for file_path in existing_files:
            self._push_file_to_queue(file_path)

    def start(self, process_existing: bool = False) -> None:
        """
        Start the file watcher.

        Args:
            process_existing: If True, push existing files to the queue on start.
        """
        if self.running:
            logger.warning("File watcher is already running.")
            return

        logger.info(f"Starting file watcher on {self.watch_path} for pattern {self.pattern}")

        # Process existing files if requested
        if process_existing:
            self._process_existing_files()

        # Set up the event handler and observer
        event_handler = ParquetFileHandler(
            queue_callback=self._push_file_to_queue,
            pattern=self.pattern,
        )
        self.observer = Observer()
        self.observer.schedule(event_handler, str(self.watch_path), recursive=True)

        # Start the observer in a separate thread
        self.observer.start()
        self.running = True

        # Start a thread to keep the watcher alive
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        logger.info("File watcher started.")

    def _run(self) -> None:
        """Run the observer until stopped."""
        try:
            while self.running and self.observer:
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the file watcher."""
        if not self.running:
            return

        logger.info("Stopping file watcher...")
        self.running = False

        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None

        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None

        logger.info("File watcher stopped.")

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


def start_file_watcher(
    watch_path: Optional[Path] = None,
    pattern: Optional[str] = None,
    poll_interval: Optional[float] = None,
    process_existing: bool = False,
    queue_manager: Optional[QueueManager] = None,
) -> FileWatcher:
    """
    Convenience function to start a file watcher.

    Args:
        watch_path: Directory to watch.
        pattern: File pattern to watch.
        poll_interval: Polling interval.
        process_existing: Whether to process existing files.
        queue_manager: Queue manager to use for pushing file paths.

    Returns:
        The started FileWatcher instance.
    """
    watcher = FileWatcher(
        watch_path=watch_path,
        pattern=pattern,
        poll_interval=poll_interval,
        queue_manager_instance=queue_manager,
    )
    watcher.start(process_existing=process_existing)
    return watcher
