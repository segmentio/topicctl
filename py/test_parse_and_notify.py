from typing import Sequence

import pytest

from parse_and_notify import NewTopic, UpdatedTopic, make_markdown_table

TABLE_TESTS = [
    pytest.param([], [], "%%%\n||\n||\n%%%", id="Empty table"),
    pytest.param(
        ["col1", "col2"],
        [],
        "%%%\n|col1|col2|\n|-|-|\n%%%",
        id="Table with header",
    ),
    pytest.param(
        ["col1", "col2"],
        [["val1", "val2"], ["val3", "val4"]],
        "%%%\n|col1|col2|\n|-|-|\n|val1|val2|\n|val3|val4|\n%%%",
        id="Table with header and rows",
    ),
]


@pytest.mark.parametrize("headers, content, expected", TABLE_TESTS)
def test_markdown_table(
    headers: Sequence[str],
    content: Sequence[Sequence[str | int | None]],
    expected: str,
) -> None:
    assert make_markdown_table(headers, content, False, None) == expected


NEW_TOPIC_RENDERED = """%%%
|Parameter|Value|
|-|-|
|Topic Name|my_topic|
|Action (create/update)|create|
|Partition Count|16|
|Replication Factor|3|
|cleanup.policy|delete|
|max.message.bytes|5542880|
%%%"""

EMPTY_ERROR_MESSAGE = """%%%
# ERROR - the following error occurred while processing this topic:
this is an error

# No changes were made.%%%"""

UPDATE_CHANGES_ERROR_MESSAGE = """%%%
# ERROR - the following error occurred while processing this topic:
also an error

# The following changes were still made:

|Parameter|Old Value|New Value|
|-|-|-|
|Action (create/update)|update||
|cleanup.policy||delete|
|message.timestamp.type|CreateTime|LogAppendTime|
|max.message.bytes||REMOVED|
|Partition 0 assignments|[5, 4]|[3, 4]|
|Partition 1 assignments|[2, 6]|[5, 6]|
%%%"""


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
        "error": False,
        "errorMessage": "",
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
        "error": False,
        "errorMessage": "",
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
    assert topic.render_table() == NEW_TOPIC_RENDERED

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
        "error": True,
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
        "error": True,
        "errorMessage": "also an error",
        "dryRun": False,
    }

    topic = NewTopic.build(new_topic_content)
    topic2 = UpdatedTopic.build(updated_topic_content)
    assert isinstance(topic, NewTopic)
    # change_set is empty because no changes are in the dict
    assert topic.change_set == []

    assert topic.render_table() == EMPTY_ERROR_MESSAGE

    assert isinstance(topic2, UpdatedTopic)

    assert topic2.change_set == [
        ["Action (create/update)", "update", ""],
        ["cleanup.policy", "", "delete"],
        ["message.timestamp.type", "CreateTime", "LogAppendTime"],
        ["max.message.bytes", "", "REMOVED"],
        ["Partition 0 assignments", "[5, 4]", "[3, 4]"],
        ["Partition 1 assignments", "[2, 6]", "[5, 6]"],
    ]

    assert topic2.render_table() == UPDATE_CHANGES_ERROR_MESSAGE
