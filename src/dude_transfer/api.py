"""Automatic dispatch between file and DataFrame transfers."""

from __future__ import annotations

import os
from typing import Any

from .dataframe import send_dataframe
from .s3 import send_file
from .single_object import send_dataframe_as_single_object


def send(source: Any, destination: str, **kwargs: Any):
    """Send a local file or Spark-like DataFrame to S3.

    Path-like sources use resumable multipart upload. Objects exposing Spark's
    ``write`` interface use the distributed DataFrame path. Pass
    ``single_object=True`` to stage JSON Lines or text in parallel and compose
    one S3 object server-side.
    """

    if isinstance(source, (str, os.PathLike)):
        return send_file(source, destination, **kwargs)
    if hasattr(source, "write"):
        if kwargs.pop("single_object", False):
            return send_dataframe_as_single_object(source, destination, **kwargs)
        return send_dataframe(source, destination, **kwargs)
    raise TypeError("source must be a local path or a Spark DataFrame")
