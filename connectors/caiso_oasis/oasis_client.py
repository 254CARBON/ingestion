"""
OASIS client for CAISO OASIS SingleZip endpoints.
"""

# Re-export the existing OASIS client for reuse
from ..caiso.oasis_client import OASISClient, OASISClientConfig

__all__ = ["OASISClient", "OASISClientConfig"]
