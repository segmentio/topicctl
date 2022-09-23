[![Circle CI](https://circleci.com/gh/segmentio/topicctl.svg?style=svg&circle-token=1f82ab114a8b160f3e12291ab0fa959132c4c97c)](https://circleci.com/gh/segmentio/topicctl)
[![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/topicctl)](https://goreportcard.com/report/github.com/segmentio/topicctl)

# topicctl

A tool for easy, declarative management of Kafka topics. Includes the ability to "apply" topic
changes from YAML as well as a repl for interactive exploration of brokers, topics, consumer groups,
messages, and more.

<img width="667" alt="topicctl_screenshot2" src="https://user-images.githubusercontent.com/54862872/88094530-979db380-cb48-11ea-93e0-ed4c45aefd66.png">

## Motivation

Managing Kafka topics via the standard tooling can be tedious and error-prone; there is no
standard, declarative way to define topics (e.g., YAML files that can be checked-in to git),
and understanding the state of a cluster at any given point in time requires knowing
and using multiple, different commands with different interfaces.

We created `topicctl` to make the management of our Kafka topics more transparent and
user-friendly. The project was inspired by `kubectl` and other tools that we've used in
non-Kafka-related contexts.

See [this blog post](https://segment.com/blog/easier-management-of-Kafka-topics-with-topicctl/) for
more details.

## 🆕 Upgrading from v0

We recently revamped `topicctl` to support ZooKeeper-less cluster access as well as some
additional security options (TLS/SSL and SASL)! See
[this blog post](https://segment.com/blog/topicctl-v1/) to learn more about why and how we did
this.

All changes should be backwards compatible, but you'll need to update your cluster configs if you
want to take advantage of these new features; see the [clusters section below](#clusters) for more
details on the latest config format.

The code for the old version has been preserved in the
[v0 branch](https://github.com/segmentio/topicctl/tree/v0) if you run into problems and need to
revert.

## Related projects

Check out the [data-digger](https://github.com/segmentio/data-digger) for a command-line tool
that makes it easy to tail and summarize structured data in Kafka.

## Getting started

### Installation

Either:

1. Run `go install github.com/segmentio/topicctl/cmd/topicctl@latest`
2. Clone this repo and run `make install` in the repo root
3. Use the Docker image: `docker pull segment/topicctl`

If you use (1) or (2), the binary will be placed in `$GOPATH/bin`.

If you use Docker, you can run the tool via
`docker run -ti --rm segment/topicctl [subcommand] [flags]`. Depending on your Docker setup and
what you're trying to do, you may also need to run in the host network via `--net=host` and/or
mount local volumes with `-v`.

### Quick tour

1. Start up a 6 node Kafka cluster locally:

```
docker-compose up -d
```

2. Run the net alias script to make the broker addresses available on localhost:

```
./scripts/set_up_net_alias.sh
```

3. Apply the topic configs in [`examples/local-cluster/topics`](/examples/local-cluster/topics):

```
topicctl apply --skip-confirm examples/local-cluster/topics/*yaml
```

4. Send some test messages to the `topic-default` topic:

```
topicctl tester --broker-addr=localhost:9092 --topic=topic-default
```

5. Open up the repl (while keeping the tester running in a separate terminal):

```
topicctl repl --cluster-config=examples/local-cluster/cluster.yaml
```

6. Run some test commands:

```
get brokers
get topics
get partitions topic-default
get offsets topic-default
tail topic-default
```

7. Increase the number of partitions in the `topic-default` topic by changing the `partitions: ...`
value in
[topic-default.yaml](https://github.com/segmentio/topicctl/blob/master/examples/local-cluster/topics/topic-default.yaml#L10) to `9` and re-applying:

```
topicctl apply examples/local-cluster/topics/topic-default.yaml
```

8. Bring down the local cluster:

```
docker-compose down
```

## Usage

### Subcommands

#### apply

```
topicctl apply [path(s) to topic config(s)]
```

The `apply` subcommand ensures that the actual state of a topic in the cluster
matches the desired state in its config. If the topic doesn't exist, the tool will
create it. If the topic already exists but its cluster state is out-of-sync,
then the tool will initiate the necessary changes to bring it into compliance.

See the [Config formats](#config-formats) section below for more information on the
expected file formats.

#### bootstrap

```
topicctl [flags] bootstrap
```

The `bootstrap` subcommand creates apply topic configs from the existing topics
in a cluster. The output can be sent to either a directory (if the `--output` flag
is set) or `stdout`.

#### check

```
topicctl check [path(s) to topic config(s)]
```

The `check` command validates that each topic config has the correct fields set and is
consistent with the associated cluster config. Unless `--validate-only` is set, it then
checks the topic config against the state of the topic in the corresponding cluster.

#### get

```
topicctl get [flags] [operation]
```

The `get` subcommand lists out the instances and/or details of a particular
resource type in the cluster. Currently, the following operations are supported:

| Subcommand      | Description |
| --------- | ----------- |
| `get balance [optional topic]` | Number of replicas per broker position for topic or cluster as a whole |
| `get brokers` | All brokers in the cluster |
| `get config [broker or topic]` | Config key/value pairs for a broker or topic |
| `get groups` | All consumer groups in the cluster |
| `get lags [topic] [group]` | Lag for each topic partition for a consumer group |
| `get members [group]` | Details of each member in a consumer group |
| `get partitions [topic]` | All partitions in a topic |
| `get offsets [topic]` | Number of messages per partition along with start and end times |
| `get topics` | All topics in the cluster |

#### repl

```
topicctl repl [flags]
```

The `repl` subcommand starts up a shell that allows running the `get` and `tail`
subcommands interactively.

#### reset-offsets

```
topicctl reset-offsets [topic] [group] [flags]
```

The `reset-offsets` subcommand allows resetting the offsets for a consumer group
in a topic. The partition and offset values are set in the flags.

#### tail

```
topicctl tail [flags] [topic]
```

The `tail` subcommand tails and logs out topic messages using the APIs exposed in
[kafka-go](https://github.com/segmentio/kafka-go). It doesn't have the full functionality
of `kafkacat` (yet), but the output is prettier and it may be easier to use in some cases.

#### tester

```
topicctl tester [flags]
```

The `tester` command reads or writes test messages in a topic. For testing/demonstration purposes
only.

### Specifying the target cluster

There are three ways to specify a target cluster in the `topicctl` subcommands:

1. `--cluster-config=[path]`, where the refererenced path is a cluster configuration
  in the format expected by the `apply` command described above,
2. `--zk-addr=[zookeeper address]` and `--zk-prefix=[optional prefix for cluster in zookeeper]`, *or*
3. `--broker-addr=[bootstrap broker address]`

All subcommands support the `cluster-config` pattern. The last two are also supported
by the `get`, `repl`, `reset-offsets`, and `tail` subcommands since these can be run
independently of an `apply` workflow.

### Version compatibility

We've tested `topicctl` on Kafka clusters with versions between `0.10.1` and `2.7.1`, inclusive.

Note, however, that clusters at versions prior to `2.4.0` cannot use broker APIs for applying and
thus also require ZooKeeper API access for full functionality. See the
[cluster access details](#cluster-access-details) section below for more details.

If you run into any unexpected compatibility issues, please file a bug.

## Config formats

`topicctl` uses structured, YAML-formatted configs for clusters and topics. These are
typically source-controlled so that changes can be reviewed before being applied.

### Clusters

Each cluster associated with a managed topic must have a config. These configs can also be used
with the `get`, `repl`, `reset-offsets`, and `tail` subcommands instead of specifying a broker or
ZooKeeper address.

The following shows an annotated example:

```yaml
meta:
  name: my-cluster                      # Name of the cluster
  environment: stage                    # Cluster environment
  region: us-west-2                     # Cloud region of the cluster
  shard: 1                              # Shard index of this cluster, if it is sharded.
  description: |                        # A free-text description of the cluster (optional)
    Test cluster for topicctl.

spec:
  bootstrapAddrs:                       # One or more broker bootstrap addresses
    - my-cluster.example.com:9092
  clusterID: abc-123-xyz                # Expected cluster ID for cluster (optional,
                                        # used as safety check only)

  # ZooKeeper access settings (only required for pre-v2 clusters; leave off to force exclusive use
  # of broker APIs)
  zkAddrs:                              # One or more cluster zookeeper addresses; if these are
    - zk.example.com:2181               # omitted, then the cluster will only be accessed via
                                        # broker APIs; see the section below on cluster access for
                                        # more details.
  zkPrefix: my-cluster                  # Prefix for zookeeper nodes if using zookeeper access
  zkLockPath: /topicctl/locks           # Path used for apply locks (optional)

  # TLS/SSL settings (optional, not supported if using ZooKeeper)
  tls:
    enabled: true                       # Whether TLS is enabled
    caCertPath: path/to/ca.crt          # Path to CA cert to be used (optional)
    certPath: path/to/client.crt        # Path to client cert to be used (optional)
    keyPath: path/to/client.key         # Path to client key to be used (optional)

  # SASL settings (optional, not supported if using ZooKeeper)
  sasl:
    enabled: true                       # Whether SASL is enabled
    mechanism: SCRAM-SHA-512            # Mechanism to use; choices are AWS-MSK-IAM, PLAIN,
                                        # SCRAM-SHA-256, and SCRAM-SHA-512
    username: my-username               # SASL username; ignored for AWS-MSK-IAM
    password: my-password               # SASL password; ignored for AWS-MSK-IAM
```

Note that the `name`, `environment`, `region`, and `description` fields are used
for description/identification only, and don't appear in any API calls. They can
be set arbitrarily, provided that they match up with the values set in the
associated topic configs.

If the tool is run with the `--expand-env` option, then the cluster config will be prepreocessed
using [`os.ExpandEnv`](https://pkg.go.dev/os#ExpandEnv) at load time. The latter will replace
references of the form `$ENV_VAR_NAME` or `${ENV_VAR_NAME}` with the associated values from the
environment.

### Topics

Each topic is configured in a YAML file. The following is an
annotated example:

```yaml
meta:
  name: topics-test                     # Name of the topic
  cluster: my-cluster                   # Name of the cluster
  environment: stage                    # Environment of the cluster
  region: us-west-2                     # Region of the cluster
  description: |                        # Free-text description of the topic (optional)
    Test topic in my-cluster.

spec:
  partitions: 9                         # Number of topic partitions
  replicationFactor: 3                  # Replication factor per partition
  retentionMinutes: 360                 # Number of minutes to retain messages (optional)
  placement:
    strategy: in-zone                   # Placement strategy, see info below
    picker: randomized                  # Picker method, see info below (optional)
  settings:                             # Miscellaneous other config settings (optional)
    cleanup.policy: delete
    max.message.bytes: 5242880
```

The `cluster`, `environment`, and `region` fields are used for matching
against a cluster config and double-checking that the cluster we're applying
in is correct; they don't appear in any API calls.

See the [Kafka documentation](https://kafka.apache.org/documentation/#topicconfigs)
for more details on the parameters that can be set in the `settings` field. Note
that retention time can be set in either this section or via `retentionMinutes` but
not in both places. The latter is easier, so it's recommended.

Multiple topics can be included in the same file, separated by `---` lines, provided
that they reference the same cluster.

#### Placement strategies

The tool supports the following per-partition, replica placement strategies:

| Strategy     | Description |
| --------- | ----------- |
| `any` | Allow any replica placement |
| `balanced-leaders` | Ensure that the leaders of each partition are evenly distributed across the broker racks  |
| `in-rack` | Ensure that the followers for each partition are in the same rack as the leader; generally this is done when the leaders are already balanced, but this isn't required |
| `cross-rack` | Ensure that the replicas for each partition are all in different racks; generally this is done when the leaders are already balanced, but this isn't required |
| `static` | Specify the placement manually, via an extra `staticAssignments` field |
| `static-in-rack` | Specify the rack placement per partition manually, via an extra `staticRackAssignments` field |

#### Picker methods

There are often multiple options to pick from when updating a replica. For instance, with an
`in-rack` strategy, we can pick any replica in the target rack that isn't already used in the
partition.

Currently, `topicctl` supports the following methods for this replica "picking" process:

| Method     | Description |
| --------- | ----------- |
| `cluster-use` | Pick based on broker frequency in the topic, then break ties by looking at the frequency of each broker across all topics in the cluster |
| `lowest-index` | Pick based on broker frequency in the topic, then break ties by choosing the lowest-index broker |
| `randomized` | Pick based on broker frequency in the topic, then break ties randomly. The underlying random generator uses a consistent seed (generated from the topic name, partition, and index), so the choice won't vary between apply runs.|

If no picking method is set in the topic config, then `randomized` is used by default.

Note that these all try to achieve in-topic balance, and only vary in the case of ties.
Thus, the placements won't be significantly different in most cases.

In the future, we may add pickers that allow for some in-topic imbalance, e.g. to correct a
cluster-wide broker inbalance.

#### Rebalancing

If `apply` is run with the `--rebalance` flag set, then `topicctl` will do a full broker rebalance
after the usual apply steps. This process will check the balance of the brokers for each index
position (i.e., first, second, third, etc.) in each partition and make replacements if there
are any brokers that are significantly over- or under-represented.

The rebalance process can optionally remove brokers from a topic too. To use this feature, set the
`--to-remove` flag. Note that this flag has no effect unless `--rebalance` is also set.

Rebalancing is not done by default on all apply runs because it can be fairly disruptive and
generally shouldn't be necessary unless the topic started off in an inbalanced state or there
has been a change in the number of brokers.

## Tool safety

The `bootstrap`, `get`, `repl`, and `tail` subcommands are read-only and should never make
any changes in the cluster.

The `apply` subcommand can make changes, but under the following conditions:

1. A user confirmation is required for any mutation to the cluster
2. Topics are never deleted
3. Partitions can be added but are never removed
4. All apply runs are interruptable and idempotent (see sections below for more details)
5. Partition changes in apply runs are locked on a per-cluster basis
6. Leader changes in apply runs are locked on a per-topic basis
7. Partition replica migrations are protected via
  ["throttles"](https://kafka.apache.org/0101/documentation.html#rep-throttle)
  to prevent the cluster network from getting overwhelmed
8. Before applying, the tool checks the cluster ID against the expected value in the
  cluster config. This can help prevent errors around applying in the wrong cluster when multiple
  clusters are accessed through the same address, e.g `localhost:2181`.

The `reset-offsets` command can also make changes in the cluster and should be used carefully.

### Idempotency

Apply runs are designed to be idemponent- the effects should be the same no matter how many
times they are run, assuming everything else in the cluster remains constant (e.g., the number of
brokers, each broker's rack, etc.). Changes in other topics should generally not effect idempotency,
unless, possibly, if the topic is configured to use the `cluster-use` picker.

### Interruptibility

If an apply run is interrupted, then any in-progress broker migrations or leader elections
will continue and any applied throttles will be kept in-place. The next time the topic is applied,
the process should continue from where it left off.

## Cluster access details

### ZooKeeper vs. broker APIs

`topicctl` can interact with a cluster through either ZooKeeper or by hitting broker APIs
directly.

Broker APIs are used exclusively if the tool is run with either of the following flags:

1. `--broker-addr` *or*
2. `--cluster-config` and the cluster config doesn't specify any ZK addresses

We recommend using this "broker only" access mode for all clusters running Kafka versions >= 2.4.

In all other cases, i.e. if `--zk-addr` is specified or the cluster config has ZK addresses, then
ZooKeeper will be used for most interactions. A few operations that are not possible via ZK
will still use broker APIs, however, including:

1. Group-related `get` commands: `get groups`, `get lags`, `get members`
2. `get offsets`
3. `reset-offsets`
4. `tail`
5. `apply` with topic creation

This "mixed" mode is required for clusters running Kafka versions < 2.0.

### Limitations of broker-only access mode

There are a few limitations in the tool when using the broker APIs exclusively:

1. Only newer versions of Kafka are supported. In particular:
    - v2.0 or greater is required for read-only operations (`get brokers`, `get topics`, etc.)
    - v2.4 or greater is required for applying topic changes
2. Apply locking is not yet implemented; please be careful when applying to ensure that someone
  else isn't applying changes in the same topic at the same time.
3. The values of some dynamic broker properties, e.g. `leader.replication.throttled.rate`, are
  marked as "sensitive" and not returned via the API; `topicctl` will show the value as
  `SENSITIVE`. This appears to be fixed in v2.6.
4. Broker timestamps are not returned by the metadata API. These will be blank in the results
  of `get brokers`.
5. Applying is not fully compatible with clusters provisioned in Confluent Cloud. It appears
  that Confluent prevents arbitrary partition reassignments, among other restrictions. Read-only
  operations seem to work.

### TLS

TLS (referred to by the older name "SSL" in the Kafka documentation) is supported when running
`topicctl` in the exclusive broker API mode. To use this, either set `--tls-enabled` in the
command-line or, if using a cluster config, set `enabled: true` in the `TLS` section of
the latter.

In addition to standard TLS, the tool also supports mutual TLS using custom certs, keys, and CA
certs (in PEM format). As with the enabling of TLS, these can be configured either on the
command-line or in a cluster config. See [this config](examples/auth/cluster.yaml) for an example.

### SASL

`topicctl` supports SASL authentication when running in the exclusive broker API mode. To use this,
either set the `--sasl-mechanism` and other appropriate `--sasl-*` flags on the command line or
fill out the `SASL` section of the cluster config.

The following mechanisms can be used:

1. `AWS-MSK-IAM`
2. `PLAIN`
3. `SCRAM-SHA-256`
4. `SCRAM-SHA-512`

If using `AWS-MSK-IAM`, then `topicctl` will attempt to discover your AWS credentials in the
locations and order described [here](https://docs.aws.amazon.com/sdk-for-go/api/aws/session/).
The other mechanisms require a username and password to be set in either the cluster config
or on the command-line. See the cluster configs in the [examples/auth](/examples/auth) and
[examples/msk](/examples/msk) directories for some specific examples.

Note that SASL can be run either with or without TLS, although the former is generally more
secure.

## Development

#### Run tests

First, set up docker-compose and the associated network alias:

```
docker-compose up -d
./scripts/set_up_net_alias.sh
```

This will create a 6 node, 3 rack cluster locally with the brokers
accessible on `169.254.123.123`.

Then, run:

```
make test
```

You can change the Kafka version of the local cluster by setting the
`KAFKA_IMAGE_TAG` environment variable when running `docker-compose up -d`. See the
[`wurstmeister/kafka` dockerhub page](https://hub.docker.com/r/wurstmeister/kafka/tags) for more
details on the available versions.

#### Run against local cluster

To run the `get`, `repl`, and `tail` subcommands against the local cluster,
set `--zk-addr=localhost:2181` and leave the `--zk-prefix` flag unset.

To test out `apply`, you can use the configs in `examples/local-cluster/`.
