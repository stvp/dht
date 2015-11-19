# dht

`dht` is a [distributed hash table][wiki_dht] implementation that uses Consul
and [rendezvous hashing][wiki_rendez] to distribute keys among an arbitrary
number of distributed nodes. Because it uses rendezvous hashing to determine key
placement, removing a node from the hash table is minimally disruptive in terms
of key re-assignment.

Local hash table state is refreshed in a background goroutine using blocking
Consul API queries with the [default consistency mode][consul_api].

`dht` requires a locally-running Consul agent (version 0.5.2 or newer) with its
HTTP API listening on `127.0.0.1:8500`. `dht` nodes run a simple HTTP server on
an ephemeral port to allow Consul to periodically check that the node is still
alive.

[wiki_dht]: https://en.wikipedia.org/wiki/Distributed_hash_table
[wiki_rendez]: https://en.wikipedia.org/wiki/Rendezvous_hashing
[consul_api]: https://www.consul.io/docs/agent/http.html

## Example

```go
node1, err := dht.Join("worker", "worker-1")
node2, err := dht.Join("worker", "worker-2")

node1.Owns("some_key") // true
node2.Owns("some_key") // false

err = node1.Leave()
err = node2.Leave()
```
