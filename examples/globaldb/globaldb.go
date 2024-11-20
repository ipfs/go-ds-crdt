package main

// This is a CLI that lets you join a global permissionless CRDT-based
// database using CRDTs and IPFS.

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	crdt "github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log/v2"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	ipfslite "github.com/hsanjuan/ipfs-lite"

	multiaddr "github.com/multiformats/go-multiaddr"
)

var (
	logger    = logging.Logger("globaldb")
	topicName = "globaldb-example"
	netTopic  = "globaldb-example-net"
	config    = "globaldb-example"
)

func main() {
	daemonMode := flag.Bool("daemon", false, "Run in daemon mode")
	dataDir := flag.String("datadir", "", "Use a custom data directory")
	port := flag.String("port", "0", "Specify the TCP port to listen on")

	flag.Parse()

	if *port != "" {
		parsedPort, err := strconv.ParseUint(*port, 10, 32)
		if err != nil || parsedPort > 65535 {
			logger.Fatal("Specify a valid TCP port")
		}
	}

	// Bootstrappers are using 1024 keys. See:
	// https://github.com/ipfs/infra/issues/378
	crypto.MinRsaKeyBits = 1024

	logging.SetLogLevel("*", "error")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data := ""

	if dataDir == nil || *dataDir == "" {
		dir, err := os.MkdirTemp("", "globaldb-example")
		if err != nil {
			logger.Fatal(err)
		}
		defer os.RemoveAll(dir)
		data = dir + "/" + config
	} else {
		// check if the directory exists or create it
		_, err := os.Stat(*dataDir)
		if os.IsNotExist(err) {
			err = os.Mkdir(*dataDir, 0755)
			if err != nil {
				logger.Fatal(err)
			}
		}
		data = *dataDir + "/" + config
	}

	store, err := badger.NewDatastore(data, &badger.DefaultOptions)
	if err != nil {
		logger.Fatal(err)
	}
	defer store.Close()

	keyPath := filepath.Join(data, "key")
	var priv crypto.PrivKey
	_, err = os.Stat(keyPath)
	if os.IsNotExist(err) {
		priv, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 1)
		if err != nil {
			logger.Fatal(err)
		}
		data, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			logger.Fatal(err)
		}
		err = os.WriteFile(keyPath, data, 0400)
		if err != nil {
			logger.Fatal(err)
		}
	} else if err != nil {
		logger.Fatal(err)
	} else {
		key, err := os.ReadFile(keyPath)
		if err != nil {
			logger.Fatal(err)
		}
		priv, err = crypto.UnmarshalPrivateKey(key)
		if err != nil {
			logger.Fatal(err)
		}

	}
	pid, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		logger.Fatal(err)
	}

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/" + *port)

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		priv,
		nil,
		[]multiaddr.Multiaddr{listen},
		nil,
		ipfslite.Libp2pOptionsExtra...,
	)
	if err != nil {
		logger.Fatal(err)
	}
	defer h.Close()
	defer dht.Close()

	psub, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		logger.Fatal(err)
	}

	topic, err := psub.Join(netTopic)
	if err != nil {
		logger.Fatal(err)
	}

	netSubs, err := topic.Subscribe()
	if err != nil {
		logger.Fatal(err)
	}

	// Use a special pubsub topic to avoid disconnecting
	// from globaldb peers.
	go func() {
		for {
			msg, err := netSubs.Next(ctx)
			if err != nil {
				fmt.Println(err)
				break
			}
			h.ConnManager().TagPeer(msg.ReceivedFrom, "keep", 100)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				topic.Publish(ctx, []byte("hi!"))
				time.Sleep(20 * time.Second)
			}
		}
	}()

	ipfs, err := ipfslite.New(ctx, store, nil, h, dht, nil)
	if err != nil {
		logger.Fatal(err)
	}

	psubCtx, psubCancel := context.WithCancel(ctx)
	pubsubBC, err := crdt.NewPubSubBroadcaster(psubCtx, psub, topicName)
	if err != nil {
		logger.Fatal(err)
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logger
	opts.RebroadcastInterval = 5 * time.Second
	opts.PutHook = func(k ds.Key, v []byte) {
		fmt.Printf("Added: [%s] -> %s\n", k, string(v))

	}
	opts.DeleteHook = func(k ds.Key) {
		fmt.Printf("Removed: [%s]\n", k)
	}

	crdt, err := crdt.New(store, ds.NewKey("crdt"), ipfs, pubsubBC, opts)
	if err != nil {
		logger.Fatal(err)
	}
	defer crdt.Close()
	defer psubCancel()

	fmt.Println("Bootstrapping...")

	bstr, _ := multiaddr.NewMultiaddr("/ip4/94.130.135.167/tcp/33123/ipfs/12D3KooWFta2AE7oiK1ioqjVAKajUJauZWfeM7R413K7ARtHRDAu")
	inf, _ := peer.AddrInfoFromP2pAddr(bstr)
	list := append(ipfslite.DefaultBootstrapPeers(), *inf)
	ipfs.Bootstrap(list)
	h.ConnManager().TagPeer(inf.ID, "keep", 100)

	fmt.Printf(`
Peer ID: %s
Topic: %s
Data Folder: %s
Listen addresses:
%s

Ready!
`,
		pid, topicName, data, listenAddrs(h),
	)

	if *daemonMode {
		fmt.Println("Running in daemon mode")
		go func() {
			for {
				fmt.Printf(
					"%s - %d connected peers - %d peers in topic\n",
					time.Now().Format(time.Stamp),
					len(connectedPeers(h)),
					len(topic.ListPeers()),
				)
				time.Sleep(10 * time.Second)
			}
		}()
		signalChan := make(chan os.Signal, 20)
		signal.Notify(
			signalChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGHUP,
		)
		<-signalChan
		return
	}

	commands := `
> (l)ist                      -> list items in the store
> (g)get <key>                -> get value for a key
> (p)ut <key> <value>         -> store value on a key
> (d)elete <key>              -> delete a key
> (c)onnect <multiaddr>       -> connect a multiaddr
> print                       -> Print DAG
> debug <on/off/peers/subs>   -> enable/disable debug logging
                                 show connected peers
                                 show pubsub subscribers
> exit                        -> quit


`
	fmt.Printf("%s", commands)

	fmt.Printf("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		fields := strings.Fields(text)
		if len(fields) == 0 {
			fmt.Printf("> ")
			continue
		}

		cmd := fields[0]

		switch cmd {
		case "exit", "quit":
			return
		case "?", "help", "h":
			fmt.Printf("%s", commands)
			fmt.Printf("> ")
			continue
		case "debug":
			if len(fields) < 2 {
				fmt.Println("debug <on/off/peers/subs>")
				fmt.Println("> ")
				continue
			}
			st := fields[1]
			switch st {
			case "on":
				logging.SetLogLevel("globaldb", "debug")
			case "off":
				logging.SetLogLevel("globaldb", "error")
			case "peers":
				for _, p := range connectedPeers(h) {
					addrs, err := peer.AddrInfoToP2pAddrs(p)
					if err != nil {
						logger.Warn(err)
						continue
					}
					for _, a := range addrs {
						fmt.Println(a)
					}
				}
			case "subs":
				for _, p := range topic.ListPeers() {
					fmt.Println(p.String())
				}
			}
		case "l", "list":
			q := query.Query{}
			results, err := crdt.Query(ctx, q)
			if err != nil {
				printErr(err)
			}
			for r := range results.Next() {
				if r.Error != nil {
					printErr(err)
					continue
				}
				fmt.Printf("[%s] -> %s\n", r.Key, string(r.Value))
			}
		case "g", "get":
			if len(fields) < 2 {
				fmt.Println("get <key>")
				fmt.Printf("> ")
				continue
			}
			k := ds.NewKey(fields[1])
			v, err := crdt.Get(ctx, k)
			if err != nil {
				printErr(err)
				continue
			}
			fmt.Printf("[%s] -> %s\n", k, string(v))
		case "p", "put":
			if len(fields) < 3 {
				fmt.Println("put <key> <value>")
				fmt.Printf("> ")
				continue
			}
			k := ds.NewKey(fields[1])
			v := strings.Join(fields[2:], " ")
			err := crdt.Put(ctx, k, []byte(v))
			if err != nil {
				printErr(err)
				continue
			}
		case "d", "delete":
			if len(fields) < 2 {
				fmt.Println("delete <key>")
				fmt.Printf("> ")
				continue
			}
			k := ds.NewKey(fields[1])
			err := crdt.Delete(ctx, k)
			if err != nil {
				printErr(err)
				continue
			}
		case "c", "connect":
			if len(fields) < 2 {
				fmt.Println("connect <mulitaddr>")
				fmt.Printf("> ")
				continue
			}
			ma, err := multiaddr.NewMultiaddr(fields[1])
			if err != nil {
				printErr(err)
				continue
			}
			peerInfo, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				printErr(err)
				continue
			}
			h.Peerstore().AddAddr(peerInfo.ID, peerInfo.Addrs[0], 300)
			err = h.Connect(ctx, *peerInfo)
			if err != nil {
				printErr(err)
				continue
			}
		case "print":
			crdt.PrintDAG(ctx)
		}
		fmt.Printf("> ")
	}
}

func printErr(err error) {
	fmt.Println("error:", err)
	fmt.Println("> ")
}

func connectedPeers(h host.Host) []*peer.AddrInfo {
	var pinfos []*peer.AddrInfo
	for _, c := range h.Network().Conns() {
		pinfos = append(pinfos, &peer.AddrInfo{
			ID:    c.RemotePeer(),
			Addrs: []multiaddr.Multiaddr{c.RemoteMultiaddr()},
		})
	}
	return pinfos
}

func listenAddrs(h host.Host) string {
	var addrs []string
	for _, c := range h.Addrs() {
		ma, _ := multiaddr.NewMultiaddr(c.String() + "/p2p/" + h.ID().String())
		addrs = append(addrs, ma.String())
	}
	return strings.Join(addrs, "\n")
}
