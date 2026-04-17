"""Step 6: backfill raw analysis outputs into the four-layer model.

The backfill boundary keeps Framework stable and writes Dataset, Source, and
Project updates from validated raw collection outputs.

Delegates to the backfill/ sub-package.
"""

from ..backfill.pipeline import run_backfill
