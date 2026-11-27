# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""S3 cost of a test, per test phase, reported to Argus.

The requests come from the CloudWatch request metrics of the bucket, summed over each phase and
priced with the S3 Standard list prices. The storage is the size of the bucket at the end of each
phase. The numbers are bucket-wide, so the bucket must be used by this test only, and it must
have a request-metrics configuration with the id `REQUEST_METRICS_ID` and no filter:

    aws s3api put-bucket-metrics-configuration --bucket BUCKET --id EntireBucket \
        --metrics-configuration '{"Id": "EntireBucket"}'

Every call of `S3CostMeter` is advisory: a failure is logged and swallowed, the measurement must
never change the outcome of a test.
"""

from __future__ import annotations

import functools
import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import boto3

from argus.client.generic_result import ColumnMetadata, ResultType, StaticGenericResultTable, Status
from sdcm.utils.object_storage import backup_bucket_region

LOGGER = logging.getLogger(__name__)

REQUEST_METRICS_ID = "EntireBucket"

# S3 Standard list prices in us-east-1, USD. Traffic between EC2 and S3 of one region is free.
PUT_CLASS_PRICE_PER_1K = 0.005
GET_CLASS_PRICE_PER_1K = 0.0004
STORAGE_PRICE_PER_GB_MONTH = 0.023

PUT_CLASS_METRICS = ("PutRequests", "PostRequests", "ListRequests")  # PutRequests includes UploadPart
GET_CLASS_METRICS = ("GetRequests", "HeadRequests", "SelectRequests")
OTHER_METRICS = ("DeleteRequests", "AllRequests", "BytesUploaded", "BytesDownloaded")  # DELETE is free
METRICS = (*PUT_CLASS_METRICS, *GET_CLASS_METRICS, *OTHER_METRICS)

#: CloudWatch gets the request metrics of a minute a few minutes after it ended.
METRICS_DELAY = 300
TOTAL_ROW = "total"
#: The bucket of `storage = {'type': 'S3', 'bucket': 'NAME', ...}` in a CREATE KEYSPACE statement.
S3_KEYSPACE_BUCKET = re.compile(r"'type'\s*:\s*'S3'\s*,\s*'bucket'\s*:\s*'([^']+)'")
GIB = 2**30  # S3 bills the storage per GB of 2^30 bytes


class S3CostResult(StaticGenericResultTable):
    class Meta:
        name = "S3 object storage cost"
        description = (
            "S3 requests of the bucket per test phase (CloudWatch request metrics) at the S3 Standard list "
            "prices, and the size of the bucket at the end of the phase"
        )
        Columns = [
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION),
            ColumnMetadata(name="PUT, POST, LIST requests", unit="", type=ResultType.INTEGER),
            ColumnMetadata(name="GET, HEAD requests", unit="", type=ResultType.INTEGER),
            ColumnMetadata(name="DELETE requests", unit="", type=ResultType.INTEGER),
            ColumnMetadata(name="uploaded", unit="GiB", type=ResultType.FLOAT),
            ColumnMetadata(name="downloaded", unit="GiB", type=ResultType.FLOAT),
            ColumnMetadata(name="request cost", unit="USD", type=ResultType.FLOAT),
            ColumnMetadata(name="request cost per hour", unit="USD/h", type=ResultType.FLOAT),
            ColumnMetadata(name="bucket size", unit="GiB", type=ResultType.FLOAT),
            ColumnMetadata(name="storage cost", unit="USD/month", type=ResultType.FLOAT),
        ]


@dataclass
class Phase:
    name: str
    start: datetime
    end: datetime | None = None
    bucket_bytes: int | None = None


def request_cost(sums: dict[str, float]) -> float:
    """USD for the requests in `sums`, a CloudWatch metric name -> number of requests mapping."""
    put_class = sum(sums.get(metric, 0) for metric in PUT_CLASS_METRICS)
    get_class = sum(sums.get(metric, 0) for metric in GET_CLASS_METRICS)
    return put_class / 1000 * PUT_CLASS_PRICE_PER_1K + get_class / 1000 * GET_CLASS_PRICE_PER_1K


def phase_row(start: datetime, end: datetime, sums: dict[str, float], bucket_bytes: int | None) -> dict:
    """Values of one `S3CostResult` row, by column name."""
    seconds = (end - start).total_seconds()
    cost = request_cost(sums)
    row = {
        "duration": int(seconds),
        "PUT, POST, LIST requests": int(sum(sums.get(metric, 0) for metric in PUT_CLASS_METRICS)),
        "GET, HEAD requests": int(sum(sums.get(metric, 0) for metric in GET_CLASS_METRICS)),
        "DELETE requests": int(sums.get("DeleteRequests", 0)),
        "uploaded": round(sums.get("BytesUploaded", 0) / GIB, 2),
        "downloaded": round(sums.get("BytesDownloaded", 0) / GIB, 2),
        "request cost": round(cost, 2),
        "request cost per hour": round(cost / (seconds / 3600), 2) if seconds > 0 else 0.0,
    }
    if bucket_bytes is not None:
        row["bucket size"] = round(bucket_bytes / GIB, 2)
        row["storage cost"] = round(bucket_bytes / GIB * STORAGE_PRICE_PER_GB_MONTH, 2)
    return row


def _advisory(method):
    """Log and swallow any failure of `method`, and skip it when the meter has no bucket."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.bucket:
            return None
        try:
            return method(self, *args, **kwargs)
        except Exception:  # noqa: BLE001
            LOGGER.warning("S3 cost of bucket '%s': %s failed", self.bucket, method.__name__, exc_info=True)
            return None

    return wrapper


class S3CostMeter:
    """Measures the S3 cost of a bucket per test phase and sends it to Argus.

    Without a bucket every method does nothing, so a test calls it without a condition.

    Example:
        meter = S3CostMeter.from_params(self.params, self.test_config.argus_client())
        meter.check()
        meter.start_phase("preload")
        ...
        meter.end_phase()
        meter.report(settle=True)
    """

    def __init__(self, bucket: str | None, region: str | None = None, argus_client=None):
        self.bucket = bucket
        self.region = region
        self.argus_client = argus_client
        self.phases: list[Phase] = []

    @classmethod
    def from_params(cls, params, argus_client) -> S3CostMeter:
        """Meter of the bucket that `pre_create_keyspace` puts a keyspace on, or a meter that does nothing."""
        statements = params.get("pre_create_keyspace") or ""
        match = S3_KEYSPACE_BUCKET.search(statements if isinstance(statements, str) else " ".join(statements))
        bucket = match.group(1) if match else None
        return cls(bucket=bucket, region=backup_bucket_region(params) if bucket else None, argus_client=argus_client)

    @functools.cached_property
    def _s3(self):
        return boto3.client("s3", region_name=self.region)

    @functools.cached_property
    def _cloudwatch(self):
        return boto3.client("cloudwatch", region_name=self.region)

    @_advisory
    def check(self) -> None:
        """Fail early, in the log, when the runner cannot read what the report needs."""
        self._s3.get_bucket_metrics_configuration(Bucket=self.bucket, Id=REQUEST_METRICS_ID)
        self._s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        now = datetime.now(tz=UTC)
        self._request_sums(now - timedelta(minutes=5), now)
        LOGGER.info("S3 cost of bucket '%s' (%s) will be reported to Argus", self.bucket, self.region)

    @_advisory
    def start_phase(self, name: str) -> None:
        phase = Phase(name=name, start=datetime.now(tz=UTC))
        self.phases.append(phase)
        LOGGER.info("S3 cost of bucket '%s': phase '%s' started at %s", self.bucket, name, phase.start.isoformat())

    @_advisory
    def end_phase(self) -> None:
        """Close the last phase, record the size of the bucket, and send what is known so far."""
        phase = self.phases[-1]
        phase.end = datetime.now(tz=UTC)
        # Logged before anything that can fail, so the report can be rebuilt from CloudWatch by hand.
        LOGGER.info("S3 cost of bucket '%s': phase '%s' ended at %s", self.bucket, phase.name, phase.end.isoformat())
        phase.bucket_bytes = self._bucket_bytes()
        LOGGER.info("S3 cost of bucket '%s': %s bytes at the end of '%s'", self.bucket, phase.bucket_bytes, phase.name)
        self.report()

    @_advisory
    def report(self, settle: bool = False) -> None:
        """Send one row per finished phase, and a total row from the first start to the last end.

        A row replaces the one sent before, so the last minutes of a phase, which CloudWatch did not
        have yet, are filled in by the next report. `settle` waits for them first: use it for the
        last report of the test.
        """
        finished = [phase for phase in self.phases if phase.end]
        if not finished or not self.argus_client:
            return
        if settle:
            time.sleep(METRICS_DELAY)
        table = S3CostResult()
        rows = [(phase.name, phase.start, phase.end, phase.bucket_bytes) for phase in finished]
        if len(finished) > 1:
            rows.append((TOTAL_ROW, finished[0].start, finished[-1].end, finished[-1].bucket_bytes))
        for name, start, end, bucket_bytes in rows:
            values = phase_row(start, end, self._request_sums(start, end), bucket_bytes)
            LOGGER.info("S3 cost of bucket '%s', %s: %s", self.bucket, name, values)
            for column, value in values.items():
                table.add_result(column=column, row=name, value=value, status=Status.UNSET)
        self.argus_client.submit_results(table)

    def _request_sums(self, start: datetime, end: datetime) -> dict[str, float]:
        """Sum of every request metric over the 1-minute datapoints from `start` to `end`."""
        queries = [
            {
                "Id": f"m{index}",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/S3",
                        "MetricName": metric,
                        "Dimensions": [
                            {"Name": "BucketName", "Value": self.bucket},
                            {"Name": "FilterId", "Value": REQUEST_METRICS_ID},
                        ],
                    },
                    "Period": 60,
                    "Stat": "Sum",
                },
            }
            for index, metric in enumerate(METRICS)
        ]
        sums = dict.fromkeys(METRICS, 0.0)
        paginator = self._cloudwatch.get_paginator("get_metric_data")
        for page in paginator.paginate(MetricDataQueries=queries, StartTime=start, EndTime=end):
            for result in page["MetricDataResults"]:
                sums[METRICS[int(result["Id"][1:])]] += sum(result["Values"])
        return sums

    def _bucket_bytes(self) -> int:
        size = 0
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            size += sum(obj["Size"] for obj in page.get("Contents", []))
        return size
