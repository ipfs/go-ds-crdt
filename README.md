# go-ds-crdt

> A distributed [go-datastore](https://github.com/ipfs/go-datastore)
> implementation using Merkle-CRDTs.

`go-ds-crdt` is a key-value store implementation using Merkle CRDTs, as
described in
[the paper by Héctor Sanjuán, Samuli Pöyhtäri and Pedro Teixeira](https://github.com/ipfs/research-CRDT/issues/45).
It satisfies the
[`Datastore`](https://godoc.org/github.com/ipfs/go-datastore#Datastore),
[`ThreadSafeDatastore`](https://godoc.org/github.com/ipfs/go-datastore#ThreadSafeDatastore)
and [`Batching`](https://godoc.org/github.com/ipfs/go-datastore#Batching)
interfaces from `go-datastore`.

This implementation Work In Progress and highly experimental at the moment.

Internally it uses a delta-CRDT Add-Wins Observed-Removed set. The current
value for a key is the one with highest priority. Priorities are defined as
the height of the Merkle-CRDT node in which the key was introduced.

## Usage

`go-ds-crdt` needs:
  * A user-provided, thread-safe, [`go-datastore`](https://github.com/ipfs/go-datastore)
    implementation to be used as permanent storage.
  * A user-defined `Broadcaster` component to broadcast and receive updates
    from a set of replicas.
  * A user-defined `DAGSyncer` component to publish and retrieve Merkle DAGs

The permanent storage layout is optimized for KV stores with fast indexes and
key-prefix support.

See https://godoc.org/github.com/ipfs/go-ds-crdt for more information.

## Captain

This project is captained by @hsanjuan.

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.

