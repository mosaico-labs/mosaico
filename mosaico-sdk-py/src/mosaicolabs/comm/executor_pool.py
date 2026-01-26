"""
Executor Pool Module.

This module manages a pool of `ThreadPoolExecutor` instances.
It is primarily used to offload CPU-bound tasks (like data serialization) or
blocking I/O waits from the main application thread during asynchronous operations.
"""

from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from typing import List, Optional

from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


_DEFAULT_EXECUTOR_POOL_SIZE = 2


class _ExecutorPool:
    """
    Manages a pool of single-worker ThreadPoolExecutors.

    This design creates distinct "lanes" for execution. By using a Round-Robin
    strategy to distribute tasks among these executors, the client ensures that
    serialization overhead is parallelized alongside network operations.
    """

    def __init__(
        self,
        *,
        pool_size: Optional[int],
    ):
        """
        Initializes the executor pool.

        Args:
            pool_size (Optional[int]): The number of executors (threads) to create.
                                       If None, defaults to `_DEFAULT_EXECUTOR_POOL_SIZE`.
        """
        self._size = pool_size or _DEFAULT_EXECUTOR_POOL_SIZE
        self._executors: List[ThreadPoolExecutor] = []
        self._iterator = None

        self._initialize_pool()

    def _initialize_pool(self):
        """
        Instantiates the executors and the cycle iterator.

        Raises:
            ValueError: If `pool_size` is less than 1.
            Exception: If an executor cannot be instantiated.
        """
        if self._size < 1:
            raise ValueError("Executor pool size must be at least 1")

        logger.debug(f"Initializing executor pool with {self._size} executors...")

        for i in range(self._size):
            try:
                # We use max_workers=1 to ensure each executor represents exactly
                # one processing thread/lane.
                self._executors.append(ThreadPoolExecutor(max_workers=1))
            except Exception as e:
                logger.error(
                    f"Failed to create executor {i + 1}/{self._size} for pool: '{e}'"
                )
                # Clean up any executors successfully created before the failure
                self.close()
                raise e

        # Create an infinite cyclic iterator for round-robin assignment
        self._iterator = cycle(self._executors)

    def get_next(self) -> ThreadPoolExecutor:
        """
        Retrieves the next executor in the pool.

        Returns:
            ThreadPoolExecutor: The next available executor instance.

        Raises:
            RuntimeError: If the pool is not initialized or has been closed.
        """
        if not self._executors or self._iterator is None:
            raise RuntimeError("executor pool is not initialized or has been closed.")
        return next(self._iterator)

    def close(self):
        """
        Shuts down all executors in the pool, freeing system resources.
        """
        for i, exec in enumerate(self._executors):
            try:
                exec.shutdown()
            except Exception as e:
                logger.warning(f"Error closing pooled executor #{i}: '{e}'")
        self._executors.clear()
        self._iterator = None
