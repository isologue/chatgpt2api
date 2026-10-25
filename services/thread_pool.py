from __future__ import annotations

import anyio.to_thread


async def apply_anyio_thread_limit(limit: int) -> None:
    """Set the default AnyIO worker-thread capacity for the current event loop."""
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = limit
