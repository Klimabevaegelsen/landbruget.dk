"""
DEPRECATED: Import from common.gcs instead.

This module is maintained for backward compatibility only.
New code should use: from common.gcs import GCSDataAccess

Migration guide:
    # Old import (deprecated)
    from unified_pipeline.util.gcs_access import GCSDataAccess

    # New import (preferred)
    from common.gcs import GCSDataAccess
"""

import warnings

warnings.warn(
    "unified_pipeline.util.gcs_access is deprecated. "
    "Use 'from common.gcs import GCSDataAccess' instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from the new canonical location
from common.gcs import GCSDataAccess, ResourceMonitor, get_duckdb_with_gcs, get_gcs_filesystem

__all__ = ["GCSDataAccess", "get_gcs_filesystem", "get_duckdb_with_gcs", "ResourceMonitor"]
