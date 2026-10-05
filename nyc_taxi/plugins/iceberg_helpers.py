"""dbt-duckdb plugin with helpers for the Iceberg materializations.

Glue refuses DROP TABLE ... with purge, so dropping an Iceberg table through DuckDB only
removes the catalog entry and leaves its files in S3. Before a table is (re)created, the
materializations call iceberg_purge_location() to empty the table's folder.

Deletes are only allowed strictly below one of the configured prefixes
(the bronze-duck / silver-duck / gold-duck roots), never on a root itself.
"""

from typing import Any, Dict, List
from urllib.parse import urlparse

import boto3
from duckdb import DuckDBPyConnection

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        prefixes = plugin_config.get("purge_allowed_prefixes") or []
        if isinstance(prefixes, str):
            prefixes = prefixes.split(",")
        self.allowed_prefixes: List[str] = [
            p.strip().rstrip("/") + "/" for p in prefixes if p and p.strip()
        ]
        for prefix in self.allowed_prefixes:
            parsed = urlparse(prefix)
            if parsed.scheme != "s3" or not parsed.netloc or parsed.path in ("", "/"):
                raise ValueError(
                    f"purge_allowed_prefixes entry {prefix!r} must be an s3://bucket/folder/ path"
                )
        self.region = plugin_config.get("region")

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function(
            "iceberg_purge_location",
            self.purge_location,
            ["VARCHAR"],
            "BIGINT",
            side_effects=True,
        )

    def purge_location(self, location: str) -> int:
        """Delete every object under `location`/ and return how many were deleted."""
        prefix = location.strip().rstrip("/") + "/"
        if ".." in prefix or "*" in prefix or "?" in prefix:
            raise ValueError(f"Refusing to purge suspicious location {location!r}")
        if not any(
            prefix.startswith(root) and len(prefix) > len(root) for root in self.allowed_prefixes
        ):
            raise ValueError(
                f"Refusing to purge {location!r}: it is not inside one of {self.allowed_prefixes}"
            )

        parsed = urlparse(prefix)
        bucket, key_prefix = parsed.netloc, parsed.path.lstrip("/")
        s3 = boto3.client("s3", region_name=self.region)
        deleted = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if not objects:
                continue
            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
            if response.get("Errors"):
                raise RuntimeError(f"Failed to delete objects under {prefix}: {response['Errors'][:5]}")
            deleted += len(objects)
        return deleted
