module github.com/ipfs/go-ds-crdt/examples/dbserver

go 1.13

// Uncomment when building with changes in the original library
replace github.com/ipfs/go-ds-crdt => ../../

require (
	github.com/hsanjuan/ipfs-lite v0.1.6
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-ds-crdt v0.1.6
	github.com/ipfs/go-log v0.0.1
	github.com/libp2p/go-libp2p v0.4.0
	github.com/libp2p/go-libp2p-connmgr v0.1.1
	github.com/libp2p/go-libp2p-core v0.2.3
	github.com/libp2p/go-libp2p-pubsub v0.1.1
	github.com/multiformats/go-multiaddr v0.1.1
	github.com/multiformats/go-multiaddr-dns v0.1.0
)
