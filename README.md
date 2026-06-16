classy
=====

An application that helps manage a cluster of Erlang nodes.

# Features

- **Cluster management**:
  sites can join together to form clusters or be removed from the cluster.
  Peer information is propagated using gossip protocol.
- **Persistent node identities**:
  nodes are assigned persistent identities that survive host name changes.
- **Service discovery**:
  sites can automatically discover one another using one of cluster discovery strategies:
  + **static**: connect to one of pre-configured nodes
  + **dns**: discover peers via `A`, `AAAA` or `SRV` record
  + **k8s**: discover peers via Kubernetes API
  + **etcd**: query [ETCD](https://etcd.io/)
- **Automatic clean up**:
  sites that remain down for a long time are automatically removed.
- **Persistence layer**:
  classy implements a standalone persistent table similar to mnesia `local_content` `disk_copies` table,
  that can be used by 3rd party applications.
- **Unique ID generation**
- **Two-phase commit** protocol implementation

# Concepts

- Site ID: a random unique identifier of the node that persists between restarts and host name changes.
- Cluster ID: a random unique identifier of the cluster.
- Run level: global system state derived from the configuration and the number of peers.
  There are the following run levels:
  + `stopped`: classy is stopped
  + `single`: classy application is running and exchanging membership information
  + `cluster`: number of known peers is >= `n_sites` configuration parameter.
  + `quorum`: number of running peers is >= `quorum` configuration parameter.


# Partition tolerance

Classy guarantees that all cluster members will eventually converge to the same state,
but earlier join and leave commands *may* override later commands.

These adverse side effects can be observed when conflicting commands are issued on different nodes faster than the nodes sync with each other.
This is most likely to happen during a network partition.

# Configuration

`classy` is configured via OTP application environment variables and callbacks.



## `classy.discovery_strategy`

Peer discovery method.

### Manual

`{manual, #{}}`

Disable automatic cluster discovery.
This is the default strategy.

### Static

`{static, #{seeds => [node()]}}`

Join to one of the nodes explicitly specified in the list.

### DNS

```erlang
{dns, #{
  name := string(),
  type => a | aaaa | srv,
  app  => string() | atom()
}}
```

Discover peers via DNS query.

- `name`: Domain name
- `type`: type of the DNS record (default: `a`)
- `app`: Node name prefix (default: `classy_autocluster:app_name()`)

Node names are derived using the following template: `App@Hostname`
where `App` is the value of `app` configuration option,
and `Hostname` is derived from the DNS response.

When `a` or `aaaa` type is used, hostnames become IP addresses.
It's recommended to use SRV records.

### K8S

```erlang
{k8s, #{
  apiserver    := string(),
  service_name := string(),
  app_name     => string(),
  address_type => ip | hostname | dns,
  namespace    => string(),
  suffix       => string()
}}
```

The **K8S discovery strategy** enables cluster nodes to discover each other by querying the Kubernetes API server.
It queries the Kubernetes API endpoint `/api/v1/namespaces/{namespace}/endpoints/{app}` to retrieve the IP addresses or hostnames of all pods associated with that service,
which are then converted into Erlang node names.

Configuration Parameters:

| Parameter      | Type   | Default                    | Description                                                                                          |
|:---------------|:-------|:---------------------------|:-----------------------------------------------------------------------------------------------------|
| `apiserver`    | String | *(Required)*               | The URL of the Kubernetes API server.                                                                |
| `service_name` | String | *(Required)*               | The name of the Kubernetes Service used for discovery.                                               |
| `app_name`     | String | Prefix of the current node | The application name used as a prefix for the generated node names.                                  |
| `address_type` | Atom   | `ip`                       | Determines the address extraction and node naming format. Supported values: `ip`, `hostname`, `dns`. |
| `namespace`    | String | `"default"`                | The Kubernetes namespace where the service is located.                                               |
| `suffix`       | String | `""`                       | An optional DNS suffix appended to the node name.                                                    |

### etcd

TODO

```erlang
{etcd, #{
  endpoints := [string()],
  prefix    := string()
}}
```

Discover peers via etcd service discovery.

- `endpoints`: List of etcd endpoints to connect to
- `prefix`: Key prefix to use for service discovery

# Setting default site and cluster

By default, classy initializes site to a random value,
and the same value is used for the cluster ID.

Business applications can override this behavior by registering `on_node_init` hook containing a call to `classy_node:maybe_init_the_site`:

```erlang
classy:on_node_init(
  fun() ->
      classy_node:maybe_init_the_site(SiteId)
  end,
  0)
```
