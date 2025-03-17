# Example GlobalDB CLI

This repository contains an example command-line interface (CLI) tool for joining a global, permissionless, CRDT-based database using CRDTs, IPFS & libp2p.

## Features

- Join a global CRDT-based database with IPFS.
- Store and retrieve key-value pairs in a distributed datastore.
- Subscribe to a pubsub topic to receive updates in real-time.
- Bootstrap and connect to other peers in the network.
- Operate in daemon mode for continuous operation.
- Simple CLI commands to interact with the database.

## Building

To build the example GlobalDB CLI, clone this repository and build the binary:

```bash
git clone https://github.com/ipfs/go-ds-crdt
cd examples/globaldb
go build -o globaldb
```

Ensure that you have Go installed and set up in your environment.

## Usage

Run the CLI with:

```bash
./globaldb [options]
```

### Options

- `-daemon`: Run in daemon mode.
- `-datadir`: Specify a directory for storing the local database and keys.

### Commands

Once running, the CLI provides the following interactive commands:

- `list`: List all items in the store.
- `get <key>`: Retrieve the value for a specified key.
- `put <key> <value>`: Store a value with a specified key.
- `connect <multiaddr>`: Connect to a peer using its multiaddress.
- `debug <on/off/peers/subs>`: Enable or disable debug logging, list connected peers, show pubsub subscribers
- `exit`: Quit the CLI.

### Example

Starting the CLI:

```bash
./globaldb -datadir /path/to/data
```

Interacting with the database:

```plaintext
> put exampleKey exampleValue
> get exampleKey
[exampleKey] -> exampleValue
> list
[exampleKey] -> exampleValue
> connect /ip4/192.168.1.3/tcp/33123/p2p/12D3KooWEkgRTTXGsmFLBembMHxVPDcidJyqFcrqbm9iBE1xhdXq
```

### Daemon Mode

To run in daemon mode, use:

```bash
./globaldb -daemon -datadir /path/to/data
```

The CLI will keep running, periodically reporting the number of connected peers and those subscribed to the crdt topic.

## Technical Details

The GlobalDB CLI leverages the following components:

- **IPFS Lite**: Provides a lightweight IPFS node for peer-to-peer networking.
- **Libp2p PubSub**: Enables decentralized communication using the GossipSub protocol.
- **CRDTs**: Ensure conflict-free synchronization of data across distributed peers.
- **Badger Datastore**: A high-performance datastore for storing key-value pairs.
