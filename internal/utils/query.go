package utils

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// IterateResults provides a standardized way to iterate over query results
// with proper error handling and resource cleanup. This consolidates the
// query iteration pattern used throughout the codebase.
func IterateResults(ctx context.Context, results query.Results, fn func(query.Result) error) error {
	// Ensure results are always closed
	defer func() {
		if err := results.Close(); err != nil {
			// Log error but don't return it as it's cleanup
			// In the future, this could use a logger if available
		}
	}()

	for result := range results.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle result errors
		if result.Error != nil {
			return WrapError("query result error", result.Error)
		}

		// Process the result
		if err := fn(result); err != nil {
			return err
		}
	}

	return nil
}

// QueryAndIterate is a convenience function that combines querying and iteration.
// It executes a query and iterates over the results in one call.
func QueryAndIterate(ctx context.Context, store ds.Datastore, q query.Query, fn func(query.Result) error) error {
	results, err := store.Query(ctx, q)
	if err != nil {
		return WrapError("failed to execute query", err)
	}

	return IterateResults(ctx, results, fn)
}

// QueryForKeys executes a query and returns all keys as a slice.
// This is useful when you need to collect all keys before processing.
func QueryForKeys(ctx context.Context, store ds.Datastore, q query.Query) ([]ds.Key, error) {
	var keys []ds.Key

	err := QueryAndIterate(ctx, store, q, func(result query.Result) error {
		keys = append(keys, ds.NewKey(result.Key))
		return nil
	})

	return keys, err
}

// QueryForValues executes a query and returns all key-value pairs as a map.
// This is useful when you need to load a set of data into memory.
func QueryForValues(ctx context.Context, store ds.Datastore, q query.Query) (map[string][]byte, error) {
	values := make(map[string][]byte)

	err := QueryAndIterate(ctx, store, q, func(result query.Result) error {
		values[result.Key] = result.Value
		return nil
	})

	return values, err
}

// CountResults executes a query and returns the number of results.
func CountResults(ctx context.Context, store ds.Datastore, q query.Query) (int, error) {
	count := 0

	err := QueryAndIterate(ctx, store, q, func(result query.Result) error {
		count++
		return nil
	})

	return count, err
}
