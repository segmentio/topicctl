#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from infra_event_notifier.notifier import Notifier

SENTRY_REGION = os.getenv("SENTRY_REGION", "unknown")


def make_markdown_table(
    headers: Sequence[str], content: Sequence[Sequence[str | int | None]]
) -> str:
    """
    Creates a markdown table given a sequence of Sequences of cells.
    """

    def make_row(row: Sequence[str | int | None]) -> str:
        content = "|".join((str(col) for col in row))
        return f"|{content}|\n"

    assert all(
        len(row) == len(headers) for row in content
    ), "Invalid table format."

    line = "-" * len(headers)
    rows = [make_row(r) for r in content]
    table = f"{make_row(headers)}{make_row(line)}{''.join(rows)}"

    return f"%%%\n{table}%%%"


@dataclass(frozen=True)
class Topic(ABC):
    name: str

    @abstractmethod
    def render_table(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class NewTopic(Topic):
    change_set: Sequence[Sequence[str | int | None]]
    error: bool

    def render_table(self) -> str:
        return make_markdown_table(
            headers=["Parameter", "Value"],
            content=[["Topic Name", self.name], *self.change_set],
        )

    @classmethod
    def build(cls, raw_content: Mapping[str, Any]) -> NewTopic:
        return NewTopic(
            name=raw_content["topic"],
            error=False,
            change_set=[
                ["Action (create/update)", "create"],
                ["Partition Count", raw_content["numPartitions"]],
                ["Replication Factor", raw_content["replicationFactor"]],
            ]
            + [
                [str(entry["name"]), str(entry["value"])]
                for entry in raw_content["configEntries"]
            ],
        )


@dataclass(frozen=True)
class UpdatedTopic(Topic):
    change_set: Sequence[Sequence[str | int | None]]
    error: bool

    def render_table(self) -> str:
        return make_markdown_table(
            headers=["Parameter", "Old Value", "New Value"],
            content=self.change_set,
        )

    @classmethod
    def build(cls, raw_content: Mapping[str, Any]) -> UpdatedTopic:
        change_set = [
            ["Action (create/update)", "update", ""],
            [
                "Partition Count",
                raw_content["numPartitions"],
                raw_content["numPartitions"],
            ],
            ["Replication Factor", "UNSUPPORTED", "UNSUPPORTED"],
        ]

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

        return UpdatedTopic(
            name=raw_content["topic"],
            error=raw_content["error"],
            change_set=change_set,
        )


@dataclass(frozen=True)
class TopicctlOutput:
    dry_run: bool
    topics: Sequence[Topic]

    @classmethod
    def build(cls, raw_content: str) -> TopicctlOutput:
        parsed = json.loads(raw_content)
        return TopicctlOutput(
            dry_run=parsed["dryRun"],
            topics=[
                *[
                    NewTopic.build(content)
                    for content in parsed["newTopics"] or []
                ],
                *[
                    UpdatedTopic.build(content)
                    for content in parsed["updatedTopics"] or []
                ],
            ],
        )


def main():
    input_data = sys.stdin.read()
    topics = TopicctlOutput.build(input_data)

    token = os.getenv("DATADOG_API_KEY")
    assert token is not None, "No Datadog token in DATADOG_API_KEY env var"
    notifier = Notifier(datadog_api_key=token)

    dry_run = "Dry run: " if topics.dry_run else ""
    tags = {
        "source": "topicctl",
        "source_category": "infra_tools",
        "sentry_region": SENTRY_REGION,
    }

    for topic in topics.topics:
        title = (
            f"{dry_run}Topicctl ran apply on topic {topic.name} "
            f"in region {SENTRY_REGION}"
        )
        text = topic.render_table()
        if len(text) > 3950:
            text = (
                "Changes exceed 4000 character limit, "
                "check topicctl logs for details on changes"
            )
        tags["topicctl_topic"] = topic.name

        notifier.notify(title=title, tags=tags, text=text, alert_type="")
        print(f"{title}", file=sys.stderr)


if __name__ == "__main__":
    main()
