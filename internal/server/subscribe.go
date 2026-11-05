package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mannysinghx/herald/internal/model"
	"github.com/mannysinghx/herald/internal/store"
)

const (
	httpWriteTimeout = 10 * time.Second // A stuck client must not hold a goroutine forever
	wsWriteTimeout   = 2 * time.Second
	wsPongGrace      = 15 * time.Second
	wsReadLimit      = 64 // Clients send nothing but pongs
)

type format int

const (
	formatJSON format = iota
	formatSSE
	formatRaw
)

func (f format) contentType() string {
	switch f {
	case formatSSE:
		return "text/event-stream; charset=utf-8"
	case formatRaw:
		return "text/plain; charset=utf-8"
	}
	return "application/x-ndjson; charset=utf-8"
}

func (f format) encode(m *model.Message) ([]byte, error) {
	if f == formatRaw {
		if m.Event == model.EventMessage {
			return []byte(strings.ReplaceAll(m.Message, "\n", " ") + "\n"), nil
		}
		return []byte("\n"), nil
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m.ForJSON()); err != nil {
		return nil, err
	}
	if f == formatJSON {
		return buf.Bytes(), nil
	}
	if m.IsNotification() {
		return fmt.Appendf(nil, "data: %s\n", buf.Bytes()), nil
	}
	// With an event name, a browser's EventSource.onmessage does not fire for this frame
	return fmt.Appendf(nil, "event: %s\ndata: %s\n", m.Event, buf.Bytes()), nil
}

type subscribeParams struct {
	poll      bool
	since     store.Since
	scheduled bool
	filter    *filter
	topics    []string
	topicsStr string
}

func (s *Server) parseSubscribe(r *http.Request) (*subscribeParams, *apiError) {
	topics, topicsStr, e := s.topicsFromPath(r.URL.Path)
	if e != nil {
		return nil, e
	}
	p := &subscribeParams{topics: topics, topicsStr: topicsStr}
	p.poll = boolParam(r, false, "x-poll", "poll", "po")
	p.scheduled = boolParam(r, false, "x-scheduled", "scheduled", "sched")
	if p.since, e = parseSince(r, p.poll); e != nil {
		return nil, e
	}
	if p.filter, e = parseFilter(r); e != nil {
		return nil, e
	}
	return p, nil
}

// handleSubscribeHTTP serves the streaming formats, and the same formats as a one-shot poll.
func (s *Server) handleSubscribeHTTP(f format) handler {
	return func(w http.ResponseWriter, r *http.Request, q *request) *apiError {
		p, e := s.parseSubscribe(r)
		if e != nil {
			return e
		}
		if e := q.visitor.addSubscription(q.exempt, s.cfg.SubscriptionLimit); e != nil {
			return e
		}
		defer q.visitor.removeSubscription()
		rc := http.NewResponseController(w)
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Content-Type", f.contentType())
		emit := func(m *model.Message) error {
			if !p.filter.pass(m) {
				return nil
			}
			encoded, err := f.encode(m)
			if err != nil {
				return err
			}
			if err := rc.SetWriteDeadline(time.Now().Add(httpWriteTimeout)); err != nil && err != http.ErrNotSupported {
				return err
			}
			if _, err := w.Write(encoded); err != nil {
				return err
			}
			if err := rc.Flush(); err != nil && err != http.ErrNotSupported {
				return err
			}
			return nil
		}
		if p.poll {
			messages, truncated, _, err := s.db.Replay(p.topics, p.since, p.scheduled, s.cfg.MessagePollSizeLimit)
			if err != nil {
				slog.Error("replay failed", "err", err)
				return errInternal
			}
			if truncated {
				h.Set("X-Messages-Truncated", "1") // Before the first write puts the headers on the wire
			}
			for _, m := range messages {
				if err := emit(m); err != nil {
					return nil
				}
			}
			return nil
		}
		h.Set("Incremental", "?1") // Asks proxies not to buffer the stream
		keepalive := func() error {
			return emit(model.New(model.EventKeepalive, p.topicsStr, ""))
		}
		if err := s.stream(r.Context(), p, q, emit, keepalive); err != nil {
			slog.Debug("subscription ended", "err", err)
		}
		return nil
	}
}

// stream runs a subscription until the client leaves, the server stops, it lags too far behind,
// or it loses access.
//
// The order is what makes it lossless: register first, then replay. A message committed before
// the replay's read is in the replay; one committed after it is in the channel; one in both is
// recognized by its position. Doing it the other way around would lose what lands in between.
func (s *Server) stream(ctx context.Context, p *subscribeParams, q *request, emit func(*model.Message) error, keepalive func() error) error {
	sub := s.hub.Subscribe(p.topics)
	defer s.hub.Unsubscribe(sub)
	if err := emit(model.New(model.EventOpen, p.topicsStr, "")); err != nil {
		return err
	}
	messages, _, high, err := s.db.Replay(p.topics, p.since, p.scheduled, s.cfg.MessagePollSizeLimit)
	if err != nil {
		return err
	}
	for _, m := range messages {
		if err := emit(m); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(s.cfg.KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.Killed():
			return fmt.Errorf("subscriber lagged more than the buffer")
		case m := <-sub.C:
			if m.Seq != 0 && m.Seq <= high {
				continue // Already replayed
			}
			if err := emit(m); err != nil {
				return err
			}
		case <-ticker.C:
			if !s.stillAllowed(q, p.topics) {
				return fmt.Errorf("access revoked")
			}
			if err := keepalive(); err != nil {
				return err
			}
		}
	}
}

// stillAllowed re-checks access, so a revoked token loses its open streams within one keepalive.
func (s *Server) stillAllowed(q *request, topics []string) bool {
	token := q.token
	if token != nil {
		current, err := s.db.TokenByID(token.ID)
		if err != nil {
			return true // A database hiccup should not disconnect everyone
		}
		if current == nil {
			return false
		}
		token = current
	}
	for _, t := range topics {
		if !s.allowed(token, t, false) {
			return false
		}
	}
	return true
}

// handleSubscribeWS serves one WebSocket. Keepalive here is a ping frame rather than an event.
// All writes happen on this goroutine, which is what gorilla/websocket requires.
func (s *Server) handleSubscribeWS(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return errWSUpgradeMissing
	}
	p, e := s.parseSubscribe(r)
	if e != nil {
		return e
	}
	if e := q.visitor.addSubscription(q.exempt, s.cfg.SubscriptionLimit); e != nil {
		return e
	}
	defer q.visitor.removeSubscription()
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		// Open to every origin on purpose: access comes from tokens, never cookies, so another
		// site's page gains nothing a plain script could not do.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	h := http.Header{"Access-Control-Allow-Origin": {"*"}}
	conn, err := upgrader.Upgrade(w, r, h)
	if err != nil {
		return nil // The upgrader has already written the error
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	// Read until the peer goes away: this is what detects a dead connection and handles pongs
	pongWait := s.cfg.KeepaliveInterval + wsPongGrace
	conn.SetReadLimit(wsReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })
	go func() {
		defer cancel()
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()
	emit := func(m *model.Message) error {
		if !p.filter.pass(m) {
			return nil
		}
		_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
		return conn.WriteJSON(m)
	}
	if p.poll {
		messages, _, _, err := s.db.Replay(p.topics, p.since, p.scheduled, s.cfg.MessagePollSizeLimit)
		if err != nil {
			slog.Error("replay failed", "err", err)
			return nil
		}
		for _, m := range messages {
			if err := emit(m); err != nil {
				return nil
			}
		}
		return nil
	}
	ping := func() error {
		_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
		return conn.WriteMessage(websocket.PingMessage, nil)
	}
	if err := s.stream(ctx, p, q, emit, ping); err != nil {
		slog.Debug("websocket subscription ended", "err", err)
	}
	return nil
}
