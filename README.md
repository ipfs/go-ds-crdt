# go-ds-crdt

> A distributed [go-datastore](https://github.com/ipfs/go-datastore)
> implementation using Merkle-CRDTs.

`go-ds-crdt` is a key-value store implementation using Merkle CRDTs, as
described in
[the paper by Héctor Sanjuán, Samuli Pöyhtäri and Pedro Teixeira](https://arxiv.org/abs/2004.00107).
It satisfies the
[`Datastore`](https://pkg.go.dev/github.com/ipfs/go-datastore#Datastore)
and [`Batching`](https://pkg.go.dev/github.com/ipfs/go-datastore#Batching)
interfaces from `go-datastore`.

This means that you can create a network of nodes that use this datastore, and 
that each key-value pair written to it will automatically replicate to every
other node. Updates can be published by any node. Network messages can be dropped, 
reordered, corrupted or duplicated. It is not necessary to know beforehand
the number of replicas participating in the system. Replicas can join and leave 
at will, without informing any other replica. There can be network partitions 
but they are resolved as soon as connectivity is re-established between replicas.

Internally it uses a delta-CRDT Add-Wins Observed-Removed set. The current
value for a key is the one with highest priority. Priorities are defined as
the height of the Merkle-CRDT node in which the key was introduced.

Implementation is independent from Broadcaster and DAG syncer layers, although the 
easiest is to use out of the box components from the IPFS stack (see below).

## Performance

Using batching, Any `go-ds-crdt` replica can easily process and sync 400 keys/s at least. The largest known deployment has 100M keys.

`go-ds-crdt` is used in production as state-synchronization layer for [IPFS Clusters](https://ipfscluster.io).

## Compaction

A `crdt.Datastore` never shrinks on its own: deleting a key adds a
tombstone rather than removing the history that preceded it, so a named
DAG's on-disk size only grows over time, even as the live key set stays
small. `Datastore.Compact(ctx, dagName)` (`Compact(ctx, "")` for the default,
unnamed DAG) addresses this: it rewrites a named DAG's current live state
(every key's winning value and priority, plus any tombstones still needed by
replicas that have not caught up) as one or more small "snapshot" DAG
blocks, and purges the history that is now redundant with them. It returns
the number of DAG blocks purged.

A fresh replica that later syncs a compacted DAG only ever needs to fetch
the snapshot block(s) -- it never has to walk the purged history -- so
compaction directly shortens how much a new or long-lagging replica needs
to download to catch up.

**Coordination-free.** `Compact` needs no coordination with anything else
writing to the target dagName, same as every other operation in this
package: concurrent `Put`s, `Delete`s, and `Compact`s -- same-view or
divergent, in any exchange order -- converge to exactly the state the
uncompacted history would have produced. Every element keeps its original
identity for life, including across compaction (a snapshot only changes
which block hosts an element's storage, recorded as an alias -- it never
changes the element's id), so a tombstone that targets an id it observed
before compaction ran keeps covering that exact element afterwards, in every
generation, on every replica. Compaction is pure representation GC, invisible
to the CRDT semantics. (Local `Put`/`Delete`/`Batch.Commit` calls on the same
`Datastore` do serialize against `Compact`, but that is an implementation
detail -- not a correctness requirement on concurrent remote writes.)

**Notes (not requirements):**

- **Two-generation tombstone carrying.** A tombstone cannot be dropped the
  moment its target's history is purged: a lagging replica may still hold
  that (soon to be purged) element and needs the tombstone to eventually
  reach it. So each compaction generation carries forward the tombstones for
  everything it purges, and only drops a tombstone once it has had one full
  generation to reach any lagging replica holding the element it kills.
- **Upgrade ordering.** An old-code replica does not know that a snapshot
  block's links are bookkeeping only (covered heads) rather than fetchable
  history: it would try to walk into now-purged blocks (and fail), while
  also mis-attributing every element's priority to the snapshot's own (much
  higher) height. Upgrade every replica that may see a given dagName's
  history to a compaction-aware version before calling `Compact` on it
  anywhere -- the same rollout rule as any wire-format addition.
- **Compact when reasonably synced.** Compaction works correctly regardless
  of how far behind other replicas are: a replica that is very far behind
  still converges correctly on reconnect (the snapshot resupplies live
  values and carries the tombstones it needs). Running it while replicas are
  reasonably caught up is purely an efficiency consideration -- it maximizes
  how much history gets folded away in one pass.
- **Receiver-side reclamation.** A replica that had already merged a DAG's
  history before a snapshot covering it arrives does not need to keep that
  history around: by default (`Options.ReclaimOnSnapshot`, on), once it has
  merged every sibling of a compaction generation it purges its own local
  copy of the history that generation covers, the same way `Compact` does on
  the compacting replica. This is best-effort (soft failures are logged, not
  fatal); `Datastore.ReclaimCompacted(ctx, dagName)` is available to trigger
  or retry reclamation manually -- for crash-missed generations, for
  deployments running with `ReclaimOnSnapshot` disabled, or for snapshots
  produced by older versions of this package.

See the `Compact` doc comment in `compact.go` for the full algorithm and the
receiving-replica behavior in each scenario (up to date, lagging, fresh).

## Usage

`go-ds-crdt` needs:
  * A user-provided, thread-safe,
    [`go-datastore`](https://github.com/ipfs/go-datastore) implementation to
    be used as permanent storage. We recommend using the
    [Pebble implementation](https://pkg.go.dev/github.com/ipfs/go-ds-pebble).
  * A user-defined `Broadcaster` component to broadcast and receive updates
    from a set of replicas. If your application uses
    [libp2p](https://libp2p.io), you can use
    [libp2p PubSub](https://pkg.go.dev/github.com/libp2p/go-libp2p-pubsub) and
    the provided
    [`PubsubBroadcaster`](https://pkg.go.dev/github.com/ipfs/go-ds-crdt?utm_source=godoc#PubSubBroadcaster).
  * A user-defined "DAG syncer" component ([`ipld.DAGService`](https://pkg.go.dev/github.com/ipfs/go-ipld-format?utm_source=godoc#DAGService)) to publish and
    retrieve Merkle DAGs to the network. For example, you can use
    [IPFS-Lite](https://github.com/hsanjuan/ipfs-lite) which casually
    satisfies this interface.

The permanent storage layout is optimized for KV stores with fast indexes and
key-prefix support.

See https://pkg.go.dev/github.com/ipfs/go-ds-crdt for more information.

## Captain

This project is captained by @hsanjuan.

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.

