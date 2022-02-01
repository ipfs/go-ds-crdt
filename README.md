# go-ds-crdt

> A distributed [go-datastore](https://github.com/ipfs/go-datastore)
> implementation using Merkle-CRDTs.

`go-ds-crdt` is a key-value store implementation using Merkle CRDTs, as
described in
[the paper by Héctor Sanjuán, Samuli Pöyhtäri and Pedro Teixeira](https://arxiv.org/abs/2004.00107).
It satisfies the
[`Datastore`](https://godoc.org/github.com/ipfs/go-datastore#Datastore)
and [`Batching`](https://godoc.org/github.com/ipfs/go-datastore#Batching)
interfaces from `go-datastore`.

Internally it uses a delta-CRDT Add-Wins Observed-Removed set. The current
value for a key is the one with highest priority. Priorities are defined as
the height of the Merkle-CRDT node in which the key was introduced.

## Usage

`go-ds-crdt` needs:
  * A user-provided, thread-safe,
    [`go-datastore`](https://github.com/ipfs/go-datastore) implementation to
    be used as permanent storage. We recommend using the
    [Badger implementation](https://godoc.org/github.com/ipfs/go-ds-badger).
  * A user-defined `Broadcaster` component to broadcast and receive updates
    from a set of replicas. If your application uses
    [libp2p](https://libp2p.io), you can use
    [libp2p PubSub](https://godoc.org/github.com/libp2p/go-libp2p-pubsub) and
    the provided
    [`PubsubBroadcaster`](https://godoc.org/github.com/ipfs/go-ds-crdt#PubSubBroadcaster).
  * A user-defined "dag syncer" component (`ipd.DAGService)` to publish and
    retrieve Merkle DAGs to the network. For example, you can use
    [IPFS-Lite](https://github.com/hsanjuan/ipfs-lite) which casually
    satisfies this interface.

The permanent storage layout is optimized for KV stores with fast indexes and
key-prefix support.

See https://godoc.org/github.com/ipfs/go-ds-crdt for more information.

## Captain

This project is captained by @hsanjuan.

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.

