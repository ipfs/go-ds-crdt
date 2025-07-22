package utils

import (
	"context"
	"errors"
	"testing"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestWrapError(t *testing.T) {
	// Test nil error
	err := WrapError("operation", nil)
	require.NoError(t, err)

	// Test error wrapping
	origErr := errors.New("original error")
	wrapped := WrapError("test operation", origErr)
	require.Error(t, wrapped)
	require.Contains(t, wrapped.Error(), "test operation")
	require.Contains(t, wrapped.Error(), "original error")
	require.ErrorIs(t, wrapped, origErr)
}

func TestWrapErrorf(t *testing.T) {
	// Test nil error
	err := WrapErrorf(nil, "operation %s", "test")
	require.NoError(t, err)

	// Test error wrapping with formatting
	origErr := errors.New("original error")
	wrapped := WrapErrorf(origErr, "test operation for %s with value %d", "key", 42)
	require.Error(t, wrapped)
	require.Contains(t, wrapped.Error(), "test operation for key with value 42")
	require.Contains(t, wrapped.Error(), "original error")
	require.ErrorIs(t, wrapped, origErr)
}

func TestCheckCID(t *testing.T) {
	// Test undefined CID
	err := CheckCID(cid.Undef)
	require.Error(t, err)
	require.Contains(t, err.Error(), "undefined")

	// Test defined CID
	c := cid.NewCidV1(cid.Raw, []byte("test"))
	err = CheckCID(c)
	require.NoError(t, err)
}

func TestCheckCIDf(t *testing.T) {
	// Test undefined CID with custom message
	err := CheckCIDf(cid.Undef, "custom error for %s", "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "custom error for test")

	// Test defined CID
	c := cid.NewCidV1(cid.Raw, []byte("test"))
	err = CheckCIDf(c, "should not see this")
	require.NoError(t, err)
}

func TestIsUndefinedCID(t *testing.T) {
	require.True(t, IsUndefinedCID(cid.Undef))

	c := cid.NewCidV1(cid.Raw, []byte("test"))
	require.False(t, IsUndefinedCID(c))
}

func TestWithBatch(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	// Test successful batch operation
	key := ds.NewKey("test")
	value := []byte("value")

	err := WithBatch(ctx, store, func(writer ds.Write) error {
		return writer.Put(ctx, key, value)
	})
	require.NoError(t, err)

	// Verify the data was written
	result, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, result)
}

func TestWithBatchOperations(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	ops := []BatchOperation{
		{Type: "put", Key: ds.NewKey("key1"), Value: []byte("value1")},
		{Type: "put", Key: ds.NewKey("key2"), Value: []byte("value2")},
		{Type: "delete", Key: ds.NewKey("key1")},
	}

	err := WithBatchOperations(ctx, store, ops)
	require.NoError(t, err)

	// Verify key1 was deleted
	_, err = store.Get(ctx, ds.NewKey("key1"))
	require.ErrorIs(t, err, ds.ErrNotFound)

	// Verify key2 exists
	result, err := store.Get(ctx, ds.NewKey("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), result)
}

func TestQueryAndIterate(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	// Setup test data
	testData := map[string][]byte{
		"prefix/key1": []byte("value1"),
		"prefix/key2": []byte("value2"),
		"other/key3":  []byte("value3"),
	}

	for k, v := range testData {
		err := store.Put(ctx, ds.NewKey(k), v)
		require.NoError(t, err)
	}

	// Test query iteration
	q := query.Query{Prefix: "prefix/"}
	var results []query.Result

	err := QueryAndIterate(ctx, store, q, func(r query.Result) error {
		results = append(results, r)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestQueryForKeys(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	// Setup test data
	keys := []string{"prefix/key1", "prefix/key2", "other/key3"}
	for _, k := range keys {
		err := store.Put(ctx, ds.NewKey(k), []byte("value"))
		require.NoError(t, err)
	}

	// Test key retrieval
	q := query.Query{Prefix: "prefix/", KeysOnly: true}
	resultKeys, err := QueryForKeys(ctx, store, q)

	require.NoError(t, err)
	require.Len(t, resultKeys, 2)
}

func TestQueryForValues(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	// Setup test data
	testData := map[string][]byte{
		"prefix/key1": []byte("value1"),
		"prefix/key2": []byte("value2"),
	}

	for k, v := range testData {
		err := store.Put(ctx, ds.NewKey(k), v)
		require.NoError(t, err)
	}

	// Test value retrieval
	q := query.Query{Prefix: "prefix/"}
	resultValues, err := QueryForValues(ctx, store, q)

	require.NoError(t, err)
	require.Len(t, resultValues, 2)
	require.Equal(t, []byte("value1"), resultValues["/prefix/key1"])
	require.Equal(t, []byte("value2"), resultValues["/prefix/key2"])
}

func TestCountResults(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ctx := context.Background()

	// Setup test data
	for i := 0; i < 5; i++ {
		key := ds.NewKey("prefix").ChildString(string(rune('a' + i)))
		err := store.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// Test count
	q := query.Query{Prefix: "prefix/"}
	count, err := CountResults(ctx, store, q)

	require.NoError(t, err)
	require.Equal(t, 5, count)
}
