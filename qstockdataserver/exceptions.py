"""Application-specific exception hierarchy and process exit codes."""

from __future__ import annotations


class QStockError(Exception):
    """Base class for expected application failures."""

    exit_code = 1


class ConfigurationError(QStockError):
    """Configuration, dependency, or schema compatibility error."""

    exit_code = 2


class TransientSourceError(QStockError):
    """A retryable Baostock/network failure that exhausted retries."""

    exit_code = 3


class FatalDataError(QStockError):
    """Downloaded or persisted market data failed strict validation."""

    exit_code = 4


class StorageError(QStockError):
    """DuckDB, filesystem, or in-memory snapshot failure."""

    exit_code = 5


class TemporarySourceAttemptError(Exception):
    """Internal single-attempt source failure; callers may retry it."""

