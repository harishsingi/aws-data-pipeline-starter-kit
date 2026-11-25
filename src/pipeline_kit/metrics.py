"""Row-count metrics: rows in / out / rejected, logged at every stage.

Every pipeline stage reports the same shape so a run's log tells the same
story whether it is the Lambda, the Glue job or the sample-data generator:
how many rows went in, how many came out, and how many were quarantined,
deduplicated or rejected -- never a bare "done".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass


@dataclass
class RowCounts:
    """Counts for one stage. All fields default to 0; fill what applies."""

    rows_in: int = 0
    rows_out: int = 0
    quarantined: int = 0
    duplicates_removed: int = 0
    rejected: int = 0

    def __post_init__(self) -> None:
        for name in ("rows_in", "rows_out", "quarantined", "duplicates_removed", "rejected"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0, got {getattr(self, name)}.")

    @property
    def rejected_rate(self) -> float:
        """Share of input rows rejected/quarantined (0.0 when nothing went in)."""
        if self.rows_in <= 0:
            return 0.0
        return (self.quarantined + self.rejected) / self.rows_in

    @property
    def rows_lost(self) -> int:
        """Input rows unaccounted for by out + quarantined + rejected + dupes."""
        return self.rows_in - (
            self.rows_out + self.quarantined + self.rejected + self.duplicates_removed
        )

    def log_summary(self, logger: logging.Logger, stage: str) -> None:
        """Log one line summarising the stage's counts."""
        logger.info(
            "%s: rows_in=%d rows_out=%d quarantined=%d duplicates_removed=%d "
            "rejected=%d rejected_rate=%.2f%% rows_lost=%d",
            stage,
            self.rows_in,
            self.rows_out,
            self.quarantined,
            self.duplicates_removed,
            self.rejected,
            self.rejected_rate * 100,
            self.rows_lost,
        )
