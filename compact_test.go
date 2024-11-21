package crdt

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
)

func TestCRDTCleanup(t *testing.T) {
	ctx := context.Background()

	// Create replicas with mock DAG service and broadcaster
	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()

	replica := replicas[0]

	// Add keys
	err := replica.Put(ctx, ds.NewKey("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to add key1: %s", err)
	}

	err = replica.Put(ctx, ds.NewKey("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("failed to add key2: %s", err)
	}

	err = replica.Put(ctx, ds.NewKey("key3"), []byte("value3"))
	if err != nil {
		t.Fatalf("failed to add key3: %s", err)
	}

	// Remove key2
	err = replica.Delete(ctx, ds.NewKey("key2"))
	if err != nil {
		t.Fatalf("failed to remove key2: %s", err)
	}

	// Update key3
	err = replica.Put(ctx, ds.NewKey("key3"), []byte("value3Updated"))
	if err != nil {
		t.Fatalf("failed to update key3: %s", err)
	}

	// Print DAG before cleanup
	t.Log("DAG before cleanup:")
	err = replica.PrintDAG()
	if err != nil {
		t.Fatalf("failed to print DAG: %s", err)
	}

	dump(t, ctx, replica)

	// Run cleanup
	t.Log("Running Cleanup...")

	// Step 1: Compact the DAG into a temporary namespace
	if err := replica.CompactDAG(ctx); err != nil {
		t.Fatalf("failed to compact DAG: %s", err)
	}

	// Print DAG after cleanup
	t.Log("DAG after cleanup:")
	err = replica.PrintDAG()
	if err != nil {
		t.Fatalf("failed to print DAG: %s", err)
	}

	// Validate the cleanup result
	t.Log("Validating cleanup...")
	if _, err := replica.Get(ctx, ds.NewKey("key2")); !errors.Is(err, ds.ErrNotFound) {
		t.Errorf("key2 should have been completely removed, but was found")
	}

	if key1Value, err := replica.Get(ctx, ds.NewKey("key1")); err != nil {
		t.Errorf("key1 should still exist, but got error: %s", err)
	} else if string(key1Value) != "value1" {
		t.Errorf("key1 should have value value1 but has %s", key1Value)
	}

	if key3Value, err := replica.Get(ctx, ds.NewKey("key3")); err != nil {
		t.Errorf("key3 should still exist, but got error: %s", err)
	} else if string(key3Value) != "value3Updated" {
		t.Errorf("key3 should have value value3Updated but has %s", key3Value)
	}

	dump(t, ctx, replica)

	err = replica.Put(ctx, ds.NewKey("key3"), []byte("value3Updated2"))
	if err != nil {
		t.Fatalf("failed to update key3: %s", err)
	}

	err = replica.Put(ctx, ds.NewKey("key2"), []byte("value2Updated2"))
	if err != nil {
		t.Fatalf("failed to update key2: %s", err)
	}

	err = replica.PrintDAG()
	if err != nil {
		t.Fatalf("failed to print DAG: %s", err)
	}

	dump(t, ctx, replica)
}

func TestMOOO(t *testing.T) {
	ctx := context.Background()

	// Create replicas with mock DAG service and broadcaster
	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()

	replica := replicas[0]

	b, err := replica.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Add keys
	err = b.Put(ctx, ds.NewKey("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to add key1: %s", err)
	}

	err = b.Put(ctx, ds.NewKey("key3"), []byte("value3Updated"))
	if err != nil {
		t.Fatalf("failed to add key3: %s", err)
	}

	err = b.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Print DAG before cleanup
	err = replica.PrintDAG()
	if err != nil {
		t.Fatalf("failed to print DAG: %s", err)
	}

	dump(t, ctx, replica)

}

func dump(t *testing.T, ctx context.Context, replica *Datastore) {
	result, err := replica.store.Query(ctx, query.Query{})
	if err != nil {
		t.Error(err)

	}
	e, err := result.Rest()
	if err != nil {
		t.Error(err)
	}
	sort.Slice(e, func(i, j int) bool {
		return e[i].Key < e[j].Key
	})

	for _, r := range e {
		fmt.Printf("%s - %s\n", r.Key, base64.URLEncoding.EncodeToString(r.Value))
	}
}

func TestAddDelAdd(t *testing.T) {
	ctx := context.Background()

	// Create replicas with mock DAG service and broadcaster
	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()

	replica := replicas[0]

	// Add keys
	err := replica.Put(ctx, ds.NewKey("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to add key1: %s", err)
	}

	err = replica.Delete(ctx, ds.NewKey("key1"))
	if err != nil {
		t.Fatalf("failed to remove key1: %s", err)
	}

	replica.startNewHead.Store(true)

	err = replica.Put(ctx, ds.NewKey("key1"), []byte("newValue"))
	if err != nil {
		t.Fatalf("failed to add key1: %s", err)
	}

	v, err := replica.Get(ctx, ds.NewKey("key1"))

	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "newValue" {
		t.Errorf("key1 should have been added, but has %s", v)
	}

	// Print DAG before cleanup
	err = replica.PrintDAG()
	if err != nil {
		t.Fatalf("failed to print DAG: %s", err)
	}

	dump(t, ctx, replica)

}
