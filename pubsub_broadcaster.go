package crdt

import (
	"context"
	"strings"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// PubsubBroadcaster implements a Broadcaster using libp2p PubSub.
type PubSubBroadcaster struct {
	ctx  context.Context
	psub *pubsub.PubSub
	subs *pubsub.Subscription
}

// NewPubsubBroadcaster returns a new broadcaster using the given PubSub and
// a topic to subscribe/broadcast to. The given context can be used to cancel
// the broadcaster.
// Please register any topic validators before creating the Broadcaster.
func NewPubSubBroadcaster(ctx context.Context, psub *pubsub.PubSub, topic string) (*PubSubBroadcaster, error) {
	subs, err := psub.Subscribe(topic)
	if err != nil {
		return nil, err
	}

	go func(ctx context.Context, subs *pubsub.Subscription) {
		select {
		case <-ctx.Done():
			subs.Cancel()

		}
	}(ctx, subs)

	return &PubSubBroadcaster{
		ctx:  ctx,
		psub: psub,
		subs: subs,
	}, nil
}

// Broadcast publishes some data.
func (pbc *PubSubBroadcaster) Broadcast(data []byte) error {
	return pbc.psub.Publish(pbc.subs.Topic(), data)
}

// Next returns published data.
func (pbc *PubSubBroadcaster) Next() ([]byte, error) {
	var msg *pubsub.Message
	var err error

	select {
	case <-pbc.ctx.Done():
		return nil, ErrNoMoreBroadcast
	default:
	}

	msg, err = pbc.subs.Next(pbc.ctx)
	if err != nil {
		if strings.Contains(err.Error(), "subscription cancelled") ||
			strings.Contains(err.Error(), "context") {
			return nil, ErrNoMoreBroadcast
		}
		return nil, err
	}

	return msg.GetData(), nil
}
