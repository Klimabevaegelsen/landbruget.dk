"""
DEPRECATED: Import from common.storage instead.

This module is maintained for backward compatibility only.
New code should use: from common.storage import StorageAccess

Migration guide:
    # Old import (deprecated)
    from unified_pipeline.util.storage_access import StorageAccess

    # New import (preferred)
    from common.storage import StorageAccess
"""

import warnings

warnings.warn(
    "unified_pipeline.util.storage_access is deprecated. "
    "Use 'from common.storage import StorageAccess' instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from the new canonical location
from common.storage import (  # noqa: E402
    ResourceMonitor,
    StorageAccess,
    get_duckdb_with_r2,
    get_r2_filesystem,
)

__all__ = ["ResourceMonitor", "StorageAccess", "get_duckdb_with_r2", "get_r2_filesystem"]
