from typing import Sequence

import pytest

from parse_and_notify import Destinations, NewTopic, UpdatedTopic, make_table

TABLE_TESTS_DATADOG = [
    pytest.param([], [], "", id="Empty table"),
    pytest.param(
        ["col1", "col2"], [], "%%%\n%%%", id="Table with just headers"
    ),
    pytest.param(
        ["col1", "col2"],
        [["val1", "val2"], ["val3", "val4"]],
        "%%%\n| col1 | col2 |\n| ---- | ---- |\n| val1 | val2 |\n| val3 | val4 |\n%%%",  # noqa
        id="Table with header and rows",
    ),
]

TABLE_TESTS_SLACK = [
    pytest.param([], [], "", id="Empty table"),
    pytest.param(["col1", "col2"], [], "", id="Table with just headers"),
    pytest.param(
        ["col1", "col2"],
        [["val1", "val2"], ["val3", "val4"]],
        "```\n| col1 | col2 |\n| ---- | ---- |\n| val1 | val2 |\n| val3 | val4 |\n```",  # noqa
        id="Table with header and rows",
    ),
]


@pytest.mark.parametrize("headers, content, expected", TABLE_TESTS_DATADOG)
def test_table_datadog(
    headers: Sequence[str],
    content: Sequence[Sequence[str | int | None]],
    expected: str,
) -> None:
    assert make_table(headers, content, None, Destinations.DATADOG) == expected


@pytest.mark.parametrize("headers, content, expected", TABLE_TESTS_SLACK)
def test_table_slack(
    headers: Sequence[str],
    content: Sequence[Sequence[str | int | None]],
    expected: str,
) -> None:
    assert make_table(headers, content, None, Destinations.SLACK) == expected


NEW_TOPIC_RENDERED = """
| Parameter              | Value    |
| ---------------------- | -------- |
| Topic Name             | my_topic |
| Action (create/update) | create   |
| Partition Count        | 16       |
| Replication Factor     | 3        |
| cleanup.policy         | delete   |
| max.message.bytes      | 5542880  |
"""
DD_NEW_TOPIC_RENDERED = f"%%%{NEW_TOPIC_RENDERED}%%%"
SLACK_NEW_TOPIC_RENDERED = f"```{NEW_TOPIC_RENDERED}```"

DD_ERROR_MESSAGE = """
# ERROR - the following error occurred while processing this topic:
this is an error
"""
DD_EMPTY_ERROR_MESSAGE = f"%%%{DD_ERROR_MESSAGE}\n# No changes were made.\n%%%"


SLACK_ERROR_MESSAGE = (
    ":warning: *ERROR - the following error"
    " occurred while processing this topic:*\n"
    "this is an error\n"
)
SLACK_EMPTY_ERROR_MESSAGE = (
    f"{SLACK_ERROR_MESSAGE}\n:warning: *No changes were made.*\n"
)

UPDATE_CHANGES = """
| Parameter               | Old Value  | New Value     |
| ----------------------- | ---------- | ------------- |
| Action (create/update)  | update     |               |
| cleanup.policy          |            | delete        |
| message.timestamp.type  | CreateTime | LogAppendTime |
| max.message.bytes       |            | REMOVED       |
| Partition 0 assignments | [5, 4]     | [3, 4]        |
| Partition 1 assignments | [2, 6]     | [5, 6]        |
"""
DD_UPDATE_ERROR = f"%%%{DD_ERROR_MESSAGE}\n# The following changes were still made:{UPDATE_CHANGES}%%%"  # noqa
SLACK_UPDATE_ERROR = f"{SLACK_ERROR_MESSAGE}\n:warning: *The following changes were still made:*\n```{UPDATE_CHANGES}```"  # noqa


def test_topicctl() -> None:
    new_topic_content = {
        "action": "create",
        "topic": "my_topic",
        "numPartitions": 16,
        "replicationFactor": 3,
        "configEntries": [
            {"name": "cleanup.policy", "value": "delete"},
            {"name": "max.message.bytes", "value": "5542880"},
        ],
        "errorMessage": None,
        "dryRun": False,
    }
    updated_topic_content = {
        "action": "update",
        "topic": "topic-default",
        "numPartitions": None,
        "newConfigEntries": [{"name": "cleanup.policy", "value": "delete"}],
        "updatedConfigEntries": [
            {
                "name": "message.timestamp.type",
                "current": "CreateTime",
                "updated": "LogAppendTime",
            }
        ],
        "missingKeys": ["max.message.bytes"],
        "replicaAssignments": [
            {
                "partition": 0,
                "currentReplicas": [5, 4],
                "updatedReplicas": [3, 4],
            },
            {
                "partition": 1,
                "currentReplicas": [2, 6],
                "updatedReplicas": [5, 6],
            },
        ],
        "errorMessage": None,
        "dryRun": False,
    }

    topic = NewTopic.build(new_topic_content)
    topic2 = UpdatedTopic.build(updated_topic_content)
    assert isinstance(topic, NewTopic)
    assert topic.change_set == [
        ["Action (create/update)", "create"],
        ["Partition Count", 16],
        ["Replication Factor", 3],
        ["cleanup.policy", "delete"],
        ["max.message.bytes", "5542880"],
    ]
    assert topic.render_table(Destinations.DATADOG) == DD_NEW_TOPIC_RENDERED
    assert topic.render_table(Destinations.SLACK) == SLACK_NEW_TOPIC_RENDERED

    assert isinstance(topic2, UpdatedTopic)

    assert topic2.change_set == [
        ["Action (create/update)", "update", ""],
        ["cleanup.policy", "", "delete"],
        ["message.timestamp.type", "CreateTime", "LogAppendTime"],
        ["max.message.bytes", "", "REMOVED"],
        ["Partition 0 assignments", "[5, 4]", "[3, 4]"],
        ["Partition 1 assignments", "[2, 6]", "[5, 6]"],
    ]


def test_topicctl_errors() -> None:
    new_topic_content = {
        "action": "create",
        "topic": "my_topic",
        "numPartitions": None,
        "replicationFactor": None,
        "configEntries": [],
        "errorMessage": "this is an error",
        "dryRun": False,
    }
    updated_topic_content = {
        "action": "update",
        "topic": "topic-default",
        "numPartitions": None,
        "newConfigEntries": [{"name": "cleanup.policy", "value": "delete"}],
        "updatedConfigEntries": [
            {
                "name": "message.timestamp.type",
                "current": "CreateTime",
                "updated": "LogAppendTime",
            }
        ],
        "missingKeys": ["max.message.bytes"],
        "replicaAssignments": [
            {
                "partition": 0,
                "currentReplicas": [5, 4],
                "updatedReplicas": [3, 4],
            },
            {
                "partition": 1,
                "currentReplicas": [2, 6],
                "updatedReplicas": [5, 6],
            },
        ],
        "errorMessage": "this is an error",
        "dryRun": False,
    }

    topic = NewTopic.build(new_topic_content)
    topic2 = UpdatedTopic.build(updated_topic_content)
    assert isinstance(topic, NewTopic)
    # change_set is empty because no changes are in the dict
    assert topic.change_set == []

    assert topic.render_table(Destinations.DATADOG) == DD_EMPTY_ERROR_MESSAGE
    assert topic.render_table(Destinations.SLACK) == SLACK_EMPTY_ERROR_MESSAGE

    assert isinstance(topic2, UpdatedTopic)

    assert topic2.change_set == [
        ["Action (create/update)", "update", ""],
        ["cleanup.policy", "", "delete"],
        ["message.timestamp.type", "CreateTime", "LogAppendTime"],
        ["max.message.bytes", "", "REMOVED"],
        ["Partition 0 assignments", "[5, 4]", "[3, 4]"],
        ["Partition 1 assignments", "[2, 6]", "[5, 6]"],
    ]

    assert topic2.render_table(Destinations.DATADOG) == DD_UPDATE_ERROR
    assert topic2.render_table(Destinations.SLACK) == SLACK_UPDATE_ERROR
