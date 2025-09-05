#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence

from infra_event_notifier.datadog_notifier import DatadogNotifier
from infra_event_notifier.slack_notifier import SlackNotifier


class Destinations(Enum):
    DATADOG = "datadog"
    SLACK = "slack"


# DD event max length is 4000 chars,
# slack msg max length is 3000 chars,
# we check 100 lower to leave room for titles
DATADOG_MAX_LENGTH = 3900
SLACK_MAX_LENGTH = 2900


SENTRY_REGION = os.getenv("SENTRY_REGION", "unknown")


def make_table(
    headers: Sequence[str],
    content: Sequence[Sequence[str | int | None]],
    error_message: str | None,
    destination: str,
) -> str:
    """
    Formats an ASCII table for slack message since slack's
    markdown support is lacking
    """
    if not headers and not content:
        return ""

    def make_row(
        row: Sequence[str | int | None], max_width: Sequence[int]
    ) -> str:
        content = "|".join(
            (
                " " + str(col).ljust(max_width[i]) + " "
                for i, col in enumerate(row)
            )
        )
        return f"|{content}|\n"

    assert all(
        len(row) == len(headers) for row in content
    ), "Invalid table format."
    # get max width of columns
    num_cols = len(headers)
    max_width = [len(h) for h in headers]
    for i in range(num_cols):
        for row in content:
            max_width[i] = max(max_width[i], len(str(row[i])))

    line = ["-" * (width) for width in max_width]
    rows = [make_row(r, max_width) for r in content]
    # only create table body if changes actually occurred
    # (if no changes then `content` = [["Topic Name", {name}]], hence len > 1)
    table = (
        (
            f"{make_row(headers, max_width)}"
            + f"{make_row(line, max_width)}"
            + f"{''.join(rows)}"
        )
        if len(content) > 1
        else ""
    )
    if destination == Destinations.SLACK and table:
        table = f"```\n{table}```"

    if error_message is not None:
        error_header = (
            "ERROR - the following error occurred while processing this topic:"
        )
        error_footer = (
            "The following changes were still made:"
            if len(content) > 1
            else "No changes were made."
        )
        if destination == Destinations.DATADOG:
            error_header = f"# {error_header}\n"
            error_footer = f"# {error_footer}\n"
        elif destination == Destinations.SLACK:
            error_header = f":warning: *{error_header}*\n"
            error_footer = f":warning: *{error_footer}*\n"
        table = error_header + f"{error_message}\n\n" + error_footer + table

    if destination == Destinations.DATADOG:
        table = f"%%%\n{table}%%%"

    return table


@dataclass(frozen=True)
class Topic(ABC):
    name: str

    @abstractmethod
    def render_table(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class NewTopic(Topic):
    change_set: Sequence[Sequence[str | int | None]]
    dry_run: bool
    error_message: str | None
    name: str

    def render_table(self, destination: str) -> str:
        return make_table(
            headers=["Parameter", "Value"],
            content=[["Topic Name", self.name], *self.change_set],
            error_message=self.error_message,
            destination=destination,
        )

    @classmethod
    def build(cls, raw_content: Mapping[str, Any]) -> NewTopic:
        change_set = [["Action (create/update)", "create"]]
        if raw_content["numPartitions"]:
            change_set.extend(
                [["Partition Count", raw_content["numPartitions"]]]
            )
        if raw_content["replicationFactor"]:
            change_set.extend(
                [["Replication Factor", raw_content["replicationFactor"]]]
            )
        change_set += [
            [str(entry["name"]), str(entry["value"])]
            for entry in raw_content["configEntries"]
        ]
        # if nothing changed, report no changes
        if len(change_set) == 1:
            change_set = []
        return NewTopic(
            name=raw_content["topic"],
            dry_run=raw_content["dryRun"],
            error_message=raw_content["errorMessage"],
            change_set=change_set,
        )


@dataclass(frozen=True)
class UpdatedTopic(Topic):
    change_set: Sequence[Sequence[str | int | None]]
    dry_run: bool
    error_message: str | None
    name: str

    def render_table(self, destination: str) -> str:
        return make_table(
            headers=["Parameter", "Old Value", "New Value"],
            content=self.change_set,
            error_message=self.error_message,
            destination=destination,
        )

    @classmethod
    def build(cls, raw_content: Mapping[str, Any]) -> UpdatedTopic:
        change_set = [["Action (create/update)", "update", ""]]

        if (
            raw_content["numPartitions"]
            and raw_content["numPartitions"]["current"]
            and raw_content["numPartitions"]["updated"]
        ):
            change_set.extend(
                [
                    [
                        "Partition Count",
                        raw_content["numPartitions"]["current"],
                        raw_content["numPartitions"]["updated"],
                    ]
                ]
            )

        if raw_content["newConfigEntries"]:
            change_set.extend(
                [
                    [str(entry["name"]), "", str(entry["value"])]
                    for entry in raw_content["newConfigEntries"] or []
                ]
            )

        if raw_content["updatedConfigEntries"]:
            change_set.extend(
                [
                    [
                        str(entry["name"]),
                        str(entry["current"]),
                        str(entry["updated"]),
                    ]
                    for entry in raw_content["updatedConfigEntries"]
                ]
            )

        if raw_content["missingKeys"]:
            change_set.extend(
                [
                    [str(entry), "", "REMOVED"]
                    for entry in raw_content["missingKeys"] or []
                ]
            )
        if raw_content["replicaAssignments"]:
            assignments = raw_content["replicaAssignments"]
            change_set.extend(
                [
                    [
                        f"Partition {p['partition']} assignments",
                        str(p["currentReplicas"]),
                        str(p["updatedReplicas"]),
                    ]
                    for p in assignments
                ]
            )
        if len(change_set) == 1:
            change_set = []

        return UpdatedTopic(
            name=raw_content["topic"],
            dry_run=raw_content["dryRun"],
            error_message=raw_content["errorMessage"],
            change_set=change_set,
        )


def main():
    dd_token = os.getenv("DATADOG_API_KEY")
    slack_secret = os.getenv("TOPICCTL_WEBHOOK_SECRET")
    slack_url = os.getenv("ENG_PIPES_URL")
    assert dd_token is not None, "No Datadog token in DATADOG_API_KEY env var"
    assert (
        slack_secret is not None
    ), "No HMAC secret in TOPICCTL_WEBHOOK_SECRET env var"

    dd_notifier = DatadogNotifier(datadog_api_key=dd_token)
    slack_notifier = SlackNotifier(
        eng_pipes_key=slack_secret, eng_pipes_url=slack_url
    )

    for line in sys.stdin:
        print("DEBUG read line: {line}")
        topic = json.loads(line)
        action = topic["action"]
        topic_content = (
            NewTopic.build(topic)
            if action == "create"
            else UpdatedTopic.build(topic)
        )

        tags = {
            "source": "topicctl",
            "source_tool": "topicctl",
            "source_category": "infra-tools",
            "sentry_region": SENTRY_REGION,
        }

        dry_run = "Dry run: " if topic_content.dry_run else ""
        title = (
            f"{dry_run}Topicctl ran apply on topic {topic_content.name} "
            f"in region {SENTRY_REGION}"
        )
        dd_table = topic_content.render_table(Destinations.DATADOG)
        slack_table = topic_content.render_table(Destinations.SLACK)

        if len(dd_table) > DATADOG_MAX_LENGTH:
            dd_table = dd_table[:(DATADOG_MAX_LENGTH)] + "\n..."
        if len(slack_table) > SLACK_MAX_LENGTH:
            slack_table = slack_table[:(SLACK_MAX_LENGTH)] + "\n..."
        tags["topicctl_topic"] = topic_content.name

        dd_notifier.send(title=title, body=dd_table, tags=tags, alert_type="")
        slack_notifier.send(title=title, body=slack_table)
        print(f"{title}", file=sys.stderr)


if __name__ == "__main__":
    main()
