"""URI resolution for Parquet data sources.

Supports file://, gs://, and s3:// schemes. Cloud SDKs are only
imported when their scheme is used, keeping the import footprint small.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def resolve_source_uri(source_uri: str) -> str:
    """Validate and resolve a source URI for PyArrow to read.

    Args:
        source_uri: URI string with scheme (file://, gs://, s3://).

    Returns:
        A path or URI that PyArrow can open directly.

    Raises:
        ValueError: Unsupported URI scheme.
        FileNotFoundError: Local file does not exist.
    """
    if source_uri.startswith("file://"):
        local_path = source_uri[len("file://") :]
        if not os.path.exists(local_path):
            raise FileNotFoundError(
                f"Local file not found: {local_path} (from URI: {source_uri})"
            )
        log.info("Resolved local file: %s", local_path)
        return local_path

    if source_uri.startswith("gs://"):
        log.info("Using GCS URI (requires gcsfs): %s", source_uri)
        return source_uri

    if source_uri.startswith("s3://"):
        log.info("Using S3 URI (requires s3fs): %s", source_uri)
        return source_uri

    scheme = source_uri.split("://")[0] if "://" in source_uri else source_uri
    raise ValueError(
        f"Unsupported URI scheme: '{scheme}'. "
        "Supported schemes: file://, gs://, s3://"
    )
