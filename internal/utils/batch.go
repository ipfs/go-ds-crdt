package utils

import (
	"context"

	ds "github.com/ipfs/go-datastore"
)

// WithBatch executes a function with a datastore batch if batching is supported,
// otherwise executes with the regular datastore. This consolidates the batching
// pattern used throughout the codebase.
func WithBatch(ctx context.Context, store ds.Datastore, fn func(ds.Write) error) error {
	var writer ds.Write = store
	var err error

	// Check if the datastore supports batching
	batchingDs, supportsBatching := store.(ds.Batching)
	if supportsBatching {
		writer, err = batchingDs.Batch(ctx)
		if err != nil {
			return WrapError("failed to create batch", err)
		}
	}

	// Execute the function with the writer (batch or regular datastore)
	if err := fn(writer); err != nil {
		return err
	}

	// Commit the batch if we created one
	if supportsBatching {
		if batch, ok := writer.(ds.Batch); ok {
			if err := batch.Commit(ctx); err != nil {
				return WrapError("failed to commit batch", err)
			}
		}
	}

	return nil
}

// BatchOperation represents a single operation to be performed in a batch.
type BatchOperation struct {
	Type  string // "put" or "delete"
	Key   ds.Key
	Value []byte // only used for "put" operations
}

// WithBatchOperations executes multiple operations in a single batch.
func WithBatchOperations(ctx context.Context, store ds.Datastore, ops []BatchOperation) error {
	return WithBatch(ctx, store, func(writer ds.Write) error {
		for _, op := range ops {
			switch op.Type {
			case "put":
				if err := writer.Put(ctx, op.Key, op.Value); err != nil {
					return WrapErrorf(err, "failed to put key %s", op.Key)
				}
			case "delete":
				if err := writer.Delete(ctx, op.Key); err != nil {
					return WrapErrorf(err, "failed to delete key %s", op.Key)
				}
			}
		}
		return nil
	})
}
