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

"""S3 requests of a test and their cost, per test phase, reported to the Costs tab of the Argus run.

The tab gets a section per phase, with one item per request family. The
requests come from the CloudWatch request metrics of the bucket, summed over each phase. The cost
is the requests at the S3 Standard list prices of us-east-1, which are constants here. The
numbers are bucket-wide, so the bucket must be used by this test only, and it must have a
request-metrics configuration with the id `REQUEST_METRICS_ID` and no filter:

    aws s3api put-bucket-metrics-configuration --bucket BUCKET --id EntireBucket \
        --metrics-configuration '{"Id": "EntireBucket"}'

Every call of `S3CostMeter` is advisory: a failure is logged and swallowed, the measurement must
never change the outcome of a test.
"""

from __future__ import annotations

import functools
import logging
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import boto3

from argus.client.types import CostItem
from sdcm.utils.cost_reporting import report_cost_items
from sdcm.utils.object_storage import backup_bucket_region

LOGGER = logging.getLogger(__name__)

REQUEST_METRICS_ID = "EntireBucket"

#: Request family, as S3 bills it -> its CloudWatch metrics and its S3 Standard list price per 1,000 requests, USD.
REQUEST_FAMILIES = {
    "PUT, POST, LIST": (("PutRequests", "PostRequests", "ListRequests"), 0.005),  # PutRequests has UploadPart
    "GET, HEAD": (("GetRequests", "HeadRequests", "SelectRequests"), 0.0004),
    "DELETE": (("DeleteRequests",), 0.0),
}
METRICS = tuple(metric for metrics, _ in REQUEST_FAMILIES.values() for metric in metrics)

#: CloudWatch gets the request metrics of a minute a few minutes after it ended.
METRICS_DELAY = 300
#: The bucket of `storage = {'type': 'S3', 'bucket': 'NAME', ...}` in a CREATE KEYSPACE statement.
S3_KEYSPACE_BUCKET = re.compile(r"'type'\s*:\s*'S3'\s*,\s*'bucket'\s*:\s*'([^']+)'")


@dataclass
class Phase:
    name: str
    start: datetime
    end: datetime | None = None
    #: CloudWatch metric name -> sum over the phase, set when the phase is reported.
    sums: dict[str, float] | None = None


def cost_items(phases: list[Phase]) -> list[CostItem]:
    """Items for the Costs tab of Argus, which shows them as a section per reported phase.

    The number of requests goes to the pricing tier of the item, the only free column of the tab.
    Argus keys an item by its name, and the sections have the same item names. So the name of an
    item of the section number N ends with N spaces, which a browser does not show.

    Example of a section (item, pricing tier, cost):
        S3 preload                            $13.93
        DELETE             771,957             $0.00
        GET, HEAD          2,355,410           $0.94
        PUT, POST, LIST    2,597,299          $12.99
    """
    items = []
    for order, phase in enumerate(phases):
        for family, (metrics, price) in REQUEST_FAMILIES.items():
            requests = int(sum(phase.sums[metric] for metric in metrics))
            items.append(
                CostItem(
                    name=family + " " * order,
                    category=f"s3_{phase.name}",
                    cost=round(requests / 1000 * price, 4),
                    pricing_tier=f"{requests:,}",
                )
            )
    return items


def _advisory(method):
    """Log and swallow any failure of `method`, and skip it when the meter has no bucket."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.bucket:
            return None
        try:
            return method(self, *args, **kwargs)
        except Exception:  # noqa: BLE001
            LOGGER.warning("S3 usage of bucket '%s': %s failed", self.bucket, method.__name__, exc_info=True)
            return None

    return wrapper


class S3CostMeter:
    """Measures the S3 usage of a bucket per test phase and sends it to Argus.

    Without a bucket every method does nothing, so a test calls it without a condition.

    Example:
        meter = S3CostMeter.from_params(self.params, self.test_config.argus_client())
        meter.check()
        meter.start_phase("preload")
        ...
        meter.end_phase()
        meter.wait()
    """

    def __init__(self, bucket: str | None, region: str | None = None, argus_client=None):
        self.bucket = bucket
        self.region = region
        self.argus_client = argus_client
        self.phases: list[Phase] = []
        self._reports: list[threading.Timer] = []

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
        now = datetime.now(tz=UTC)
        self._request_sums(now - timedelta(minutes=5), now)
        LOGGER.info("S3 usage of bucket '%s' (%s) will be reported to Argus", self.bucket, self.region)

    @_advisory
    def start_phase(self, name: str) -> None:
        phase = Phase(name=name, start=datetime.now(tz=UTC))
        self.phases.append(phase)
        LOGGER.info("S3 usage of bucket '%s': phase '%s' started at %s", self.bucket, name, phase.start.isoformat())

    @_advisory
    def end_phase(self) -> None:
        """Close the last phase and report it in `METRICS_DELAY` seconds, when CloudWatch has its last minutes.

        The report waits in the background, so the next phase starts at once.
        """
        phase = self.phases[-1]
        if phase.end:
            return
        phase.end = datetime.now(tz=UTC)
        LOGGER.info("S3 usage of bucket '%s': phase '%s' ended at %s", self.bucket, phase.name, phase.end.isoformat())
        report = threading.Timer(METRICS_DELAY, self._report, args=(phase,))
        report.daemon = True
        report.start()
        self._reports.append(report)

    @_advisory
    def wait(self) -> None:
        """Wait for the reports of all the ended phases: call it before the test ends."""
        for report in self._reports:
            report.join()

    @_advisory
    def _report(self, phase: Phase) -> None:
        phase.sums = self._request_sums(phase.start, phase.end)
        LOGGER.info("S3 usage of bucket '%s', %s: %s", self.bucket, phase.name, phase.sums)
        if self.argus_client:
            report_cost_items(self.argus_client, cost_items([phase for phase in self.phases if phase.sums]))

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
