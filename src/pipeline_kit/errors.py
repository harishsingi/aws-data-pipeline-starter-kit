"""Custom exceptions for the pipeline kit.

Callers should raise (and catch) these instead of bare ``ValueError`` /
``RuntimeError`` so a failure says *what kind* of failure it is:

* :class:`ConfigurationError` -- bad config / arguments, fixable by the
  operator before any data is touched.
* :class:`ValidationError` -- a single record failed validation. The record
  is quarantined, not fatal to the batch.
* :class:`UpstreamDataError` -- an input file / snapshot is missing, empty
  or unusable; the run cannot proceed.
* :class:`DataQualityError` -- a post-transform quality gate failed; the
  run must not commit its outputs.
* :class:`RetryExhaustedError` -- a retried AWS / network call kept failing.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for all kit errors."""


class ConfigurationError(PipelineError):
    """Configuration or job arguments are missing or invalid."""


class ValidationError(PipelineError):
    """A record failed validation.

    Carries the list of error codes (see
    :func:`src.pipeline_kit.validation.validate_record`) so callers can
    quarantine the record with its reasons attached.
    """

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors: list[str] = list(errors or [])


class UpstreamDataError(PipelineError):
    """An expected input (file, snapshot, partition) is missing or empty."""


class DataQualityError(PipelineError):
    """A data-quality gate failed after the transform ran."""


class RetryExhaustedError(PipelineError):
    """A retried operation failed on every attempt."""

    def __init__(self, message: str, *, attempts: int, last_error: BaseException | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error
