"""Automatic dispatch between file and DataFrame transfers."""

from __future__ import annotations

import os
from typing import Any

from .dataframe import send_dataframe
from .s3 import send_file


def send(source: Any, destination: str, **kwargs: Any):
    """Send a local file or Spark-like DataFrame to S3.

    Path-like sources use resumable multipart upload. Objects exposing Spark's
    ``write`` interface use the distributed DataFrame path.
    """

    if isinstance(source, (str, os.PathLike)):
        return send_file(source, destination, **kwargs)
    if hasattr(source, "write"):
        return send_dataframe(source, destination, **kwargs)
    raise TypeError("source must be a local path or a Spark DataFrame")
