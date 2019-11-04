package main

// This provides a global permissionless CRDT-based database using CRDTs and
// IPFS with a simple HTTP API to insert, delete, get and list items. It uses
// an in-memory database backend. Peer IDs are generated on every run.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-pubsub"

	"github.com/hsanjuan/ipfs-lite"

	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
)

var (
	logger    = logging.Logger("crdtdb")
	listen, _ = multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	topic     = "db-example"
	httpPort  = 0
)

func init() {
	port := os.Getenv("PORT")
	if port != "" {
		listen, _ = multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/" + port)
		nPort, err := strconv.Atoi(port)
		if err != nil {
			return
		}
		httpPort = nPort + 1
	}
}

func main() {
	logging.SetLogLevel("*", "error")
	logging.SetLogLevel("ipfslite", "info")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mapds := datastore.NewMapDatastore()
	store := sync.MutexWrap(mapds)
	defer store.Close()

	var priv crypto.PrivKey
	var err error
	keyb64 := os.Getenv("KEY")
	if keyb64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(keyb64)
		if err != nil {
			logger.Fatal(err)
		}
		priv, err = crypto.UnmarshalPrivateKey(decoded)
		if err != nil {
			logger.Fatal(err)
		}
	} else {
		priv, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 1)
		if err != nil {
			logger.Fatal(err)
		}
	}
	connman := connmgr.NewConnManager(100, 100, 5*time.Second)

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		priv,
		nil,
		[]multiaddr.Multiaddr{listen},
		libp2p.ConnectionManager(connman),
	)
	defer h.Close()
	defer dht.Close()

	for _, addr := range h.Addrs() {
		fmt.Printf("%s/ipfs/%s\n", addr, h.ID())
	}
	fmt.Println()

	psub, err := pubsub.NewFloodSub(ctx, h, pubsub.WithMessageSigning(false))
	if err != nil {
		logger.Fatal(err)
	}

	ipfs, err := ipfslite.New(ctx, store, h, dht, nil)
	if err != nil {
		logger.Fatal(err)
	}

	pubsubBC, err := crdt.NewPubSubBroadcaster(ctx, psub, topic)
	if err != nil {
		logger.Fatal(err)
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logger
	opts.RebroadcastInterval = 5 * time.Second
	var tStart time.Time
	opts.PutHook = func(k datastore.Key, v []byte) {
		if tStart.IsZero() {
			tStart = time.Now()
		}
		fmt.Printf("Added: [%s] -> %s\n", k, string(v))
		if k == datastore.NewKey("time") {
			fmt.Println(time.Since(tStart))
			tStart = time.Now()
		}
	}
	opts.DeleteHook = func(k datastore.Key) {
		fmt.Printf("Removed: [%s]\n", k)
	}

	crdt, err := crdt.New(store, datastore.NewKey("crdt"), ipfs, pubsubBC, opts)
	if err != nil {
		logger.Fatal(err)
	}
	defer crdt.Close()

	bstr := os.Getenv("BOOTSTRAP")
	if len(os.Args) > 1 {
		bstr = os.Args[1]
	}

	if bstr != "" {
		fmt.Println("Bootstrapping...")
		time.Sleep(5 * time.Second)
		bstr, err := multiaddr.NewMultiaddr(bstr)
		if err != nil {
			logger.Fatal(err)
		}
		inf, _ := peer.AddrInfoFromP2pAddr(bstr)
		ipfs.Bootstrap([]peer.AddrInfo{*inf})
		connman.TagPeer(inf.ID, "keep", 100)
	}

	respondError := func(w http.ResponseWriter, err error) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error() + "\n"))
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			if r.URL.Path == "/" {
				goto LIST
			}
			kv := strings.SplitN(r.URL.Path, "/", 2)
			if len(kv) < 2 {
				http.NotFound(w, r)
				return
			}
			key := datastore.NewKey(kv[1])
			value, err := crdt.Get(key)
			if err != nil {
				respondError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(string(value) + "\n"))
			return
		case "POST":
			maddr, err := multiaddr.NewMultiaddr(r.URL.Path)
			if err != nil {
				respondError(w, err)
				return
			}
			addrInf, err := peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				respondError(w, err)
				return
			}
			conCtx, conCancel := context.WithTimeout(ctx, 30*time.Second)
			defer conCancel()
			err = h.Connect(conCtx, *addrInf)
			if err != nil {
				respondError(w, err)
				return
			}
			connman.TagPeer(addrInf.ID, "keep", 100)
			w.WriteHeader(http.StatusAccepted)
			return
		case "PUT":
			kv := strings.SplitN(r.URL.Path, "/", 3)
			if len(kv) < 3 {
				http.NotFound(w, r)
				return
			}
			key := datastore.NewKey(kv[1])
			value := kv[2]
			err := crdt.Put(key, []byte(value))
			if err != nil {
				respondError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%s:%s\n", key, value)))
			return
		case "DELETE":
			kv := strings.SplitN(r.URL.Path, "/", 2)
			if len(kv) < 2 || kv[1] == "" {
				http.NotFound(w, r)
				return
			}
			key := datastore.NewKey(kv[1])
			err := crdt.Delete(key)
			if err != nil {
				respondError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(key.String() + "\n"))
			return
		default:
			http.NotFound(w, r)
			return
		}
	LIST:
		q := query.Query{}
		results, err := crdt.Query(q)
		if err != nil {
			respondError(w, err)
		}
		w.WriteHeader(http.StatusOK)
		for r := range results.Next() {
			if r.Error != nil {
				w.Write([]byte("error: " + err.Error() + "\n"))
				continue
			}
			w.Write([]byte(fmt.Sprintf("%s:%s\n", r.Key, r.Value)))
		}
	})

	fmt.Println("Get: GET /<key>")
	fmt.Println("Put: PUT /<key>/<value>")
	fmt.Println("Delete: DELETE /<key>")
	fmt.Println("List: GET /")
	fmt.Println("Connect: POST /<multiaddress>")
	fmt.Println()

	lstr, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", httpPort))
	if err != nil {
		logger.Fatal(err)
		return
	}
	defer lstr.Close()
	fmt.Println("listening on:", lstr.Addr())
	fmt.Println()
	http.Serve(lstr, nil)
}
