"""The change index a namespace's keyed outputs live in on the new tracking
core (docs/tracking-design.md; build plan B5), opened the same way by the
engine and by a worker: on file:// or s3:// storage, with an S3 bucket's
settings read from the environment as obstore reads them for the state."""

from __future__ import annotations

import functools
import os
from urllib.parse import urlsplit

from ._native import ChangeIndex

# The environment object_store reads an S3 bucket's settings from, as obstore does for the state.
S3_ENV = {
    "AWS_ACCESS_KEY_ID": "aws_access_key_id",
    "AWS_SECRET_ACCESS_KEY": "aws_secret_access_key",
    "AWS_SESSION_TOKEN": "aws_session_token",
    "AWS_REGION": "aws_region",
    "AWS_DEFAULT_REGION": "aws_region",
    "AWS_ENDPOINT": "aws_endpoint",
    "AWS_ENDPOINT_URL": "aws_endpoint",
    "AWS_ALLOW_HTTP": "aws_allow_http",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "aws_virtual_hosted_style_request",
    "AWS_SKIP_SIGNATURE": "aws_skip_signature",
}


def index_options(url: str) -> dict:
    """The change index's store options for the objects at `url`: what the
    environment says of an S3 bucket, as obstore reads it for the state."""

    if urlsplit(url).scheme != "s3":
        return {}
    out = {}
    for name, key in S3_ENV.items():
        if (value := os.environ.get(name)) is not None:
            out.setdefault(key, value)
    return out


def open_index(url: str, cache: str | None = None) -> ChangeIndex:
    """The change index over the objects at `url` (a namespace's), with a
    disk cache of whole layer files under `cache` for a remote store."""

    scheme = urlsplit(url).scheme
    if scheme not in ("file", "s3"):
        raise ValueError(f"the tracking core keeps its layers on file:// or s3:// storage, not {scheme}://")
    disk = {"disk_dir": cache, "disk_store": url} if scheme == "s3" and cache else {}
    return ChangeIndex(url, index_options(url) or None, **disk)


@functools.cache
def shared(url: str) -> ChangeIndex:
    """One change index per namespace in this process, for whole reads."""

    return open_index(url)
