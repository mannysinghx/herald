package hub

import (
	"testing"
	"time"

	"github.com/mannysinghx/herald/internal/model"
)

func TestSlowSubscriberIsDroppedAndPublisherNotBlocked(t *testing.T) {
	h := New()
	slow := h.Subscribe([]string{"t"})
	fast := h.Subscribe([]string{"t"})
	defer h.Unsubscribe(slow)
	defer h.Unsubscribe(fast)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range BufferSize + 10 {
			h.Publish(model.New(model.EventMessage, "t", "x"))
			select { // Fast keeps up, slow reads nothing
			case <-fast.C:
			default:
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a slow subscriber blocked the publisher")
	}
	select {
	case <-slow.Killed():
	default:
		t.Fatal("slow subscriber should have been dropped")
	}
	select {
	case <-fast.Killed():
		t.Fatal("fast subscriber should not have been dropped")
	default:
	}
}

func TestUnsubscribeForgetsEmptyTopics(t *testing.T) {
	h := New()
	s := h.Subscribe([]string{"a", "b"})
	if h.Count("a") != 1 || h.Count("b") != 1 {
		t.Fatal("not subscribed")
	}
	h.Unsubscribe(s)
	if len(h.subs) != 0 {
		t.Fatalf("leaked %d topics", len(h.subs))
	}
}

func TestPublishOnlyReachesItsTopic(t *testing.T) {
	h := New()
	s := h.Subscribe([]string{"a"})
	defer h.Unsubscribe(s)
	h.Publish(model.New(model.EventMessage, "b", "x"))
	select {
	case <-s.C:
		t.Fatal("got a message for another topic")
	default:
	}
}
