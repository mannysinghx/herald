// Package hub fans messages out to the open subscriptions of this process.
package hub

import (
	"sync"

	"github.com/mannysinghx/herald/internal/model"
)

// BufferSize is how many messages a subscription may lag behind before it is dropped.
const BufferSize = 256

// Subscription is one open connection's view of its topics. A subscriber that cannot keep up is
// disconnected rather than given a gap: its client reconnects with since=<last id> and replays.
type Subscription struct {
	C      chan *model.Message
	topics []string
	killed chan struct{}
	once   sync.Once
}

// Killed is closed when the subscription was dropped for lagging.
func (s *Subscription) Killed() <-chan struct{} { return s.killed }

func (s *Subscription) kill() { s.once.Do(func() { close(s.killed) }) }

// Hub holds the subscriptions by topic. An empty topic costs nothing, so unlike a topic table
// there is nothing to expire or to cap.
type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscription]struct{}
}

// New returns an empty hub.
func New() *Hub { return &Hub{subs: map[string]map[*Subscription]struct{}{}} }

// Subscribe registers a subscription on the topics.
func (h *Hub) Subscribe(topics []string) *Subscription {
	s := &Subscription{C: make(chan *model.Message, BufferSize), topics: topics, killed: make(chan struct{})}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, t := range topics {
		if h.subs[t] == nil {
			h.subs[t] = map[*Subscription]struct{}{}
		}
		h.subs[t][s] = struct{}{}
	}
	return s
}

// Unsubscribe removes the subscription.
func (h *Hub) Unsubscribe(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, t := range s.topics {
		delete(h.subs[t], s)
		if len(h.subs[t]) == 0 {
			delete(h.subs, t)
		}
	}
}

// Publish hands the message to every subscriber of its topic and never blocks: the caller holds
// the publish lock, so one slow connection must not be able to stall all publishers.
func (h *Hub) Publish(m *model.Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs[m.Topic] {
		select {
		case s.C <- m:
		default:
			s.kill()
		}
	}
}

// Count returns the number of open subscriptions on a topic.
func (h *Hub) Count(topic string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[topic])
}
