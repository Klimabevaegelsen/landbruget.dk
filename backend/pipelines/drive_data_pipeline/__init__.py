"""Google Drive Data Pipeline package."""

# Lazy import to avoid circular dependencies during test collection
__all__ = ["main"]


def __getattr__(name):
    if name == "main":
        from .main import main

        return main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
