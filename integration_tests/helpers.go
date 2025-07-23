package integration_tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-ds-crdt"
	"github.com/stretchr/testify/require"
)

// getReplicaData extracts all key-value pairs from a replica
func getReplicaData(t testing.TB, replica *crdt.Datastore) map[string][]byte {
	ctx := context.Background()
	results, err := replica.Query(ctx, query.Query{})
	require.NoError(t, err)
	defer results.Close()

	data := make(map[string][]byte)
	for result := range results.Next() {
		require.NoError(t, result.Error)
		data[result.Key] = result.Value
	}
	return data
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		len(s) > len(substr) && s[:len(substr)] == substr ||
		len(s) > len(substr) && findInString(s, substr)
}

// findInString searches for substr within s
func findInString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
