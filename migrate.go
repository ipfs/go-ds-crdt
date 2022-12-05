package crdt

import (
	"context"
	"strings"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
)

// Version indicates which datastore version we are at. Changes in version
// may need migrations.
const Version uint = 2

func (store *Datastore) migrate(ctx context.Context, from, to uint) error {
	// chances are we can do 1 to 3 and such things in later versions.
	if from == 1 && to == 2 {
		return store.migrate1to2(ctx)
	}

	return errors.New("unknown version migration")
}

func (store *Datastore) migrate1to2(ctx context.Context) error {
	store.logger.Info("Starting migration from version 1 to 2.")
	store.logger.Info("This migration requires reprocesssing of the CRDT DAG and may take a while...")

	// delete all processed blocks
	store.logger.Info("Preparing CRDT blocks for reprocessing by deleting keys...")
	count, err := store.emptyNamespace(ctx, "/b", false)
	if err != nil {
		return err
	}
	store.logger.Infof("Finished preparing blocks for reprocessing: %d blocks", count)

	// delete everything in the keys namespace
	store.logger.Infof("Clearing the legacy keys namespace...")
	count, err = store.emptyNamespace(ctx, setNs+"/k", true)
	if err != nil {
		return err
	}
	store.logger.Infof("Finished clearing the legacy keys namespace: %d keys", count)

	store.logger.Infof("Marking store as dirty and reprocessing CRDT DAG (repair)...")
	store.MarkDirty() // in case we stop half through
	err = store.Repair()
	if err != nil {
		return err
	}
	store.SetVersion(2)
	store.logger.Infof("Migration successfully completed...")
	return nil
}

func (store *Datastore) emptyNamespace(ctx context.Context, namespace string, subkeys bool) (int, error) {
	var err error
	var writeStore ds.Write = store.store
	batchingDs, batching := writeStore.(ds.Batching)
	if batching {
		writeStore, err = batchingDs.Batch(ctx)
		if err != nil {
			return 0, err
		}
	}
	prefix := store.namespace.ChildString(namespace).String()
	store.logger.Debug("Query prefix:", prefix)
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := store.store.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer results.Close()
	count := 0
	for r := range results.Next() {
		if r.Error != nil {
			return count, r.Error
		}

		delK := r.Key
		if !subkeys {
			keyWithoutPrefix := strings.TrimPrefix(r.Key, prefix+"/")
			// abc != <key>/abc
			if ds.NewKey(r.Key).BaseNamespace() != keyWithoutPrefix {
				continue
			}
		}

		store.logger.Debugf("delete %s", delK)
		err := writeStore.Delete(ctx, ds.NewKey(delK))
		if err != nil {
			return count, err
		}
		count++
		if count%10000 == 0 {
			store.logger.Infof("%d keys deleted so far...", count)
			if batching {
				// FIXME, can we re-use the same batch all the
				// time??
				store.logger.Debug("batch commit")
				if err := writeStore.(ds.Batch).Commit(ctx); err != nil {
					return count, err
				}
			}
		}
	}
	if batching {
		store.logger.Debug("batch commit")
		if err := writeStore.(ds.Batch).Commit(ctx); err != nil {
			return count, err
		}
	}

	return count, nil
}
