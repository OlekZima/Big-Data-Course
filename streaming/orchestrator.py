"""
Orchestrator for the streaming pipeline.

This module starts all components of the streaming pipeline:
- File watcher to monitor for new Parquet files and push them to the queue
- Multiple workers to process queue items
- Handles graceful shutdown on signals
"""

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

from .config import streaming_settings
from .file_watcher import FileWatcher
from .worker import StreamingWorker

logger = logging.getLogger(__name__)


class StreamingOrchestrator:
    """Orchestrator for the streaming pipeline."""

    def __init__(
        self,
        num_workers: int = 1,
        process_existing: bool = False,
        worker_timeout: Optional[int] = None,
        watch_path: Optional[Path] = None,
        pattern: Optional[str] = None,
        poll_interval: Optional[float] = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            num_workers: Number of worker threads to start.
            process_existing: Whether to process existing files on startup.
            worker_timeout: Timeout in seconds for workers (None for no timeout).
            watch_path: Directory to watch for new files.
            pattern: File pattern to watch.
            poll_interval: Polling interval for the file watcher.
        """
        self.num_workers = num_workers
        self.process_existing = process_existing
        self.worker_timeout = worker_timeout
        self.watch_path = watch_path or streaming_settings.watch_path
        self.pattern = pattern or streaming_settings.watch_pattern
        self.poll_interval = poll_interval or streaming_settings.poll_interval

        self.file_watcher: Optional[FileWatcher] = None
        self.workers: List[StreamingWorker] = []
        self.worker_threads: List[threading.Thread] = []

        self.running = False

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def _worker_thread_func(self, worker: StreamingWorker):
        """Function to run a worker in a thread."""
        try:
            worker.run(timeout=self.worker_timeout)
        except Exception as e:
            logger.error(f"Worker {worker.worker_id} encountered an error: {e}")

    def start(self):
        """Start all components of the streaming pipeline."""
        if self.running:
            logger.warning("Orchestrator is already running.")
            return

        logger.info("Starting streaming pipeline orchestrator...")
        self.running = True

        # Start file watcher
        logger.info(f"Starting file watcher on {self.watch_path} for pattern {self.pattern}")
        self.file_watcher = FileWatcher(
            watch_path=self.watch_path,
            pattern=self.pattern,
            poll_interval=self.poll_interval,
        )
        self.file_watcher.start(process_existing=self.process_existing)

        # Start workers
        logger.info(f"Starting {self.num_workers} workers...")
        for i in range(self.num_workers):
            worker_id = f"worker-{i+1}"
            worker = StreamingWorker(worker_id=worker_id)
            self.workers.append(worker)

            thread = threading.Thread(
                target=self._worker_thread_func,
                args=(worker,),
                name=worker_id,
                daemon=True,
            )
            thread.start()
            self.worker_threads.append(thread)

        logger.info("Streaming pipeline started.")
        logger.info(f"  - Watching: {self.watch_path}")
        logger.info(f"  - Workers: {self.num_workers}")
        logger.info(f"  - Process existing: {self.process_existing}")
        if self.worker_timeout:
            logger.info(f"  - Worker timeout: {self.worker_timeout}s")

    def stop(self):
        """Stop all components of the streaming pipeline."""
        if not self.running:
            return

        logger.info("Stopping streaming pipeline...")
        self.running = False

        # Stop file watcher
        if self.file_watcher:
            logger.info("Stopping file watcher...")
            self.file_watcher.stop()
            self.file_watcher = None

        # Stop workers
        logger.info("Stopping workers...")
        for worker in self.workers:
            worker.stop()

        # Wait for worker threads to finish
        for thread in self.worker_threads:
            thread.join(timeout=5)

        self.workers.clear()
        self.worker_threads.clear()

        logger.info("Streaming pipeline stopped.")

    def run(self):
        """Run the orchestrator until shutdown signal."""
        self.start()

        try:
            # Keep the main thread alive until stopped
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


def main():
    """Main entry point for the orchestrator."""
    parser = argparse.ArgumentParser(
        description="Start the streaming pipeline orchestrator."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=streaming_settings.processing_workers,
        help=f"Number of worker threads (default: {streaming_settings.processing_workers})",
    )
    parser.add_argument(
        "--process-existing",
        action="store_true",
        help="Process existing Parquet files on startup",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=streaming_settings.processing_timeout,
        help=f"Worker timeout in seconds (default: {streaming_settings.processing_timeout})",
    )
    parser.add_argument(
        "--watch-path",
        type=Path,
        default=streaming_settings.watch_path,
        help=f"Directory to watch for new files (default: {streaming_settings.watch_path})",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=streaming_settings.watch_pattern,
        help=f"File pattern to watch (default: {streaming_settings.watch_pattern})",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=streaming_settings.poll_interval,
        help=f"Polling interval for file watcher in seconds (default: {streaming_settings.poll_interval})",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create and run the orchestrator
    orchestrator = StreamingOrchestrator(
        num_workers=args.workers,
        process_existing=args.process_existing,
        worker_timeout=args.timeout if args.timeout > 0 else None,
        watch_path=args.watch_path,
        pattern=args.pattern,
        poll_interval=args.poll_interval,
    )

    orchestrator.run()


if __name__ == "__main__":
    main()
