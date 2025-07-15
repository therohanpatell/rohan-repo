import logging
from typing import Optional


def get_logger(name: str = "OptimizedPipeline", *, level: int = logging.INFO, handler: Optional[logging.Handler] = None) -> logging.Logger:
    """Return a configured ``logging.Logger`` instance.

    The first time this function is called for a *name* it attaches the
    provided *handler* (or a default ``StreamHandler``) and sets a consistent
    message format. Subsequent calls simply return the same logger to avoid
    duplicated handlers.
    """
    logger = logging.getLogger(name)

    # Only configure the logger once per interpreter session
    if not logger.handlers:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        _handler = handler or logging.StreamHandler()
        _handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(_handler)
        logger.setLevel(level)
        logger.propagate = False  # Prevent double-logging in some environments

    return logger