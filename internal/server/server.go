// Package server implements the HTTP API: publish, subscribe, attachments and access control.
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mannysinghx/herald/internal/hub"
	"github.com/mannysinghx/herald/internal/model"
	"github.com/mannysinghx/herald/internal/store"
)

// Config is the server configuration. See DefaultConfig for the defaults.
type Config struct {
	Listen            string
	BaseURL           string
	DBPath            string
	AttachmentDir     string // Empty disables attachments
	CacheDuration     time.Duration
	KeepaliveInterval time.Duration
	DefaultAccess     store.Permission
	BehindProxy       bool
	ProxyHeader       string // Header holding the client address when BehindProxy is set
	UpstreamBaseURL   string // Forwards poll requests here, which is how iOS gets woken up
	UpstreamToken     string

	MessageSizeLimit     int
	MessagePollSizeLimit int
	TitleSizeLimit       int
	TagsSizeLimit        int
	MessageDelayMin      time.Duration
	MessageDelayMax      time.Duration
	SubscribeTopicsLimit int

	RequestBurst      int
	RequestReplenish  time.Duration
	MessageDailyLimit int
	SubscriptionLimit int
	AuthFailureBurst  int
	RateLimitExempt   []netip.Prefix

	AttachmentTotalLimit   int64
	AttachmentFileLimit    int64
	AttachmentVisitorLimit int64
	AttachmentExpiry       time.Duration
	BandwidthDailyLimit    int64

	SchedulerInterval time.Duration
	ManagerInterval   time.Duration
}

// DefaultConfig returns the defaults, which follow ntfy's so that clients see familiar limits.
func DefaultConfig() *Config {
	return &Config{
		Listen:                 ":8080",
		DBPath:                 "herald.db",
		CacheDuration:          12 * time.Hour,
		KeepaliveInterval:      45 * time.Second,
		DefaultAccess:          store.PermDeny,
		ProxyHeader:            "X-Forwarded-For",
		MessageSizeLimit:       4096,
		MessagePollSizeLimit:   10 * 1024 * 1024,
		TitleSizeLimit:         1024,
		TagsSizeLimit:          1024,
		MessageDelayMin:        10 * time.Second,
		MessageDelayMax:        3 * 24 * time.Hour,
		SubscribeTopicsLimit:   150,
		RequestBurst:           60,
		RequestReplenish:       5 * time.Second,
		MessageDailyLimit:      17280,
		SubscriptionLimit:      30,
		AuthFailureBurst:       30,
		AttachmentTotalLimit:   5 * 1024 * 1024 * 1024,
		AttachmentFileLimit:    15 * 1024 * 1024,
		AttachmentVisitorLimit: 100 * 1024 * 1024,
		AttachmentExpiry:       3 * time.Hour,
		BandwidthDailyLimit:    500 * 1024 * 1024,
		SchedulerInterval:      time.Second,
		ManagerInterval:        time.Minute,
	}
}

var (
	topicRegex       = regexp.MustCompile(`^[-_A-Za-z0-9]{1,64}$`)
	topicPathRegex   = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}$`)
	updatePathRegex  = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}/[-_A-Za-z0-9]{1,64}$`)
	clearPathRegex   = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}/[-_A-Za-z0-9]{1,64}/(read|clear)$`)
	deletePathRegex  = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}/[-_A-Za-z0-9]{1,64}/delete$`)
	publishPathRegex = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}/(publish|send|trigger)$`)
	jsonPathRegex    = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/json$`)
	ssePathRegex     = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/sse$`)
	rawPathRegex     = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/raw$`)
	wsPathRegex      = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/ws$`)
	authPathRegex    = regexp.MustCompile(`^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/auth$`)
	fileRegex        = regexp.MustCompile(`^/file/([-_A-Za-z0-9]{1,64})(?:\.[A-Za-z0-9]{1,16})?$`)
	urlRegex         = regexp.MustCompile(`^https?://`)

	// Topics whose paths the API or the web page need.
	disallowedTopics = []string{"docs", "static", "file", "app", "metrics", "account", "settings", "signup", "login", "v1"}
)

// Server is the notification service.
type Server struct {
	cfg      *Config
	db       *store.Store
	hub      *hub.Hub
	visitors *visitors

	// pubMu makes "commit, then fan out" atomic. Without it two concurrent publishes can commit
	// as 1, 2 and fan out as 2, 1, and a subscriber that has seen 2 would discard 1 as old.
	pubMu sync.Mutex

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	upstream chan struct{} // Bounds concurrent upstream forwards
	http     *http.Server
}

// New opens the database and prepares the server.
func New(cfg *Config) (*Server, error) {
	if cfg.AttachmentDir != "" {
		if err := os.MkdirAll(cfg.AttachmentDir, 0o700); err != nil {
			return nil, err
		}
	}
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:      cfg,
		db:       db,
		hub:      hub.New(),
		visitors: newVisitors(cfg),
		ctx:      ctx,
		cancel:   cancel,
		upstream: make(chan struct{}, 32),
	}
	s.http = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second, // No WriteTimeout: subscriptions are meant to stay open
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	return s, nil
}

// Serve accepts connections on l and blocks until Close.
func (s *Server) Serve(l net.Listener) error {
	s.startBackground()
	if err := s.http.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Close ends all subscriptions, stops the loops and closes the database. Clients reconnect with
// since=<last id> and lose nothing.
func (s *Server) Close() error {
	s.cancel()
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	err := s.http.Shutdown(shutdown)
	s.wg.Wait()
	return errors.Join(err, s.db.Close())
}

func (s *Server) startBackground() {
	s.loop(s.cfg.SchedulerInterval, s.sendDue)
	s.loop(s.cfg.ManagerInterval, s.manage)
}

func (s *Server) loop(every time.Duration, f func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				f()
			}
		}
	}()
}

func (s *Server) manage() {
	now := time.Now()
	s.visitors.prune(now)
	ids, err := s.db.Prune(now)
	if err != nil {
		slog.Error("pruning failed", "err", err)
		return
	}
	s.removeAttachments(ids)
	s.sweepAttachments(now)
}

type handler func(http.ResponseWriter, *http.Request, *request) *apiError

// request is what authentication learned about the caller.
type request struct {
	exempt  bool
	visitor *visitor
	token   *store.Token
}

// ServeHTTP authenticates and routes. It matches paths in the same order as ntfy, which matters:
// "/topic/publish" is both a publish path (GET) and a sequence ID path (PUT).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, e := s.authenticate(r)
	if e == nil {
		e = s.route(w, r, req)
	}
	if e != nil {
		if e.HTTP >= 500 {
			slog.Error("request failed", "path", r.URL.Path, "method", r.Method, "code", e.Code, "err", e.Message)
		}
		writeError(w, e)
	}
}

func (s *Server) route(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	p, m := r.URL.Path, r.Method
	isGet, isPut := m == http.MethodGet, m == http.MethodPut || m == http.MethodPost
	switch {
	case m == http.MethodHead && p == "/":
		return nil
	case isGet && p == "/":
		return s.handlePage(w, r, q)
	case isGet && p == "/v1/health":
		return s.handleHealth(w, r, q)
	case isGet && p == "/v1/ready":
		return s.handleHealth(w, r, q)
	case isGet && p == "/v1/config":
		return s.handleConfig(w, r, q)
	case isGet && p == "/v1/stats":
		return s.handleStats(w, r, q)
	case isGet && p == "/v1/account":
		return s.handleAccount(w, r, q)
	case (isGet || m == http.MethodHead) && fileRegex.MatchString(p) && s.cfg.AttachmentDir != "":
		return s.limited(s.handleFile)(w, r, q)
	case m == http.MethodOptions:
		return s.limited(handleOptions)(w, r, q)
	case isPut && p == "/":
		return s.limited(s.handlePublishJSON)(w, r, q)
	case isPut && (topicPathRegex.MatchString(p) || updatePathRegex.MatchString(p)):
		return s.limited(s.authorizeWrite(s.handlePublish))(w, r, q)
	case (m == http.MethodDelete && updatePathRegex.MatchString(p)) || (isGet && deletePathRegex.MatchString(p)):
		return s.limited(s.authorizeWrite(s.handleAction(model.EventMessageDelete)))(w, r, q)
	case (isGet || m == http.MethodPut) && clearPathRegex.MatchString(p):
		return s.limited(s.authorizeWrite(s.handleAction(model.EventMessageClear)))(w, r, q)
	case isGet && publishPathRegex.MatchString(p):
		return s.limited(s.authorizeWrite(s.handlePublish))(w, r, q)
	case isGet && jsonPathRegex.MatchString(p):
		return s.limited(s.authorizeRead(s.handleSubscribeHTTP(formatJSON)))(w, r, q)
	case isGet && ssePathRegex.MatchString(p):
		return s.limited(s.authorizeRead(s.handleSubscribeHTTP(formatSSE)))(w, r, q)
	case isGet && rawPathRegex.MatchString(p):
		return s.limited(s.authorizeRead(s.handleSubscribeHTTP(formatRaw)))(w, r, q)
	case isGet && wsPathRegex.MatchString(p):
		return s.limited(s.authorizeRead(s.handleSubscribeWS))(w, r, q)
	case isGet && authPathRegex.MatchString(p):
		return s.limited(s.authorizeRead(handleTopicAuth))(w, r, q)
	case isGet && topicPathRegex.MatchString(p):
		return s.handleTopicPage(w, r, q)
	}
	return errNotFound
}

// authenticate reads the credential if there is one. A wrong credential is an error rather than
// a silent downgrade to anonymous, so a typo in a token cannot pass as "no access".
func (s *Server) authenticate(r *http.Request) (*request, *apiError) {
	ip := clientIP(r, s.cfg.BehindProxy, s.cfg.ProxyHeader)
	if s.cfg.BehindProxy {
		slog.Debug("client address", "ip", ip.String(), "remote", r.RemoteAddr, "x_forwarded_for", r.Header.Get("X-Forwarded-For"), "x_real_ip", r.Header.Get("X-Real-IP"))
	}
	ipVisitor := s.visitors.get(ipKey(ip))
	q := &request{exempt: s.visitors.isExempt(ip), visitor: ipVisitor}
	secret, e := readCredential(r)
	if e != nil {
		return q, e
	}
	if secret == "" {
		return q, nil
	}
	if !q.exempt && ipVisitor.auth.Tokens() < 1 {
		return q, errLimitAuthFailure.withRetryAfter(60)
	}
	token, err := s.db.TokenBySecret(secret)
	if err != nil {
		slog.Error("token lookup failed", "err", err)
		return q, errInternal
	}
	if token == nil {
		ipVisitor.auth.Allow() // Only failures cost a token
		return q, errUnauthorized
	}
	q.token = token
	q.visitor = s.visitors.get("tok:" + token.ID)
	return q, nil
}

// readCredential returns the token from "Authorization: Bearer x", from Basic auth (where the
// password is the token, so apps that only offer user and password work), or from ?auth=, which
// holds the base64url of that header because a browser's WebSocket cannot set headers.
func readCredential(r *http.Request) (string, *apiError) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if q := r.URL.Query().Get("auth"); q != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(q, "="))
		if err != nil {
			return "", errUnauthorized
		}
		header = strings.TrimSpace(string(decoded))
	}
	lower := strings.ToLower(header)
	switch {
	case strings.HasPrefix(lower, "bearer "):
		return strings.TrimSpace(header[len("bearer "):]), nil
	case strings.HasPrefix(lower, "basic "):
		probe := &http.Request{Header: http.Header{"Authorization": {header}}}
		_, password, ok := probe.BasicAuth()
		if !ok || password == "" {
			return "", errUnauthorized
		}
		return password, nil
	}
	return "", nil // Other schemes (WebPush, vapid) are not meant for us
}

// allowed applies the token's matching rule, and otherwise the server's default.
func (s *Server) allowed(t *store.Token, topic string, write bool) bool {
	perm := s.cfg.DefaultAccess
	if t != nil {
		if p, ok := t.Permission(topic); ok {
			perm = p
		}
	}
	if write {
		return perm.CanWrite()
	}
	return perm.CanRead()
}

func (s *Server) limited(next handler) handler {
	return func(w http.ResponseWriter, r *http.Request, q *request) *apiError {
		if e := q.visitor.allowRequest(q.exempt); e != nil {
			return e
		}
		return next(w, r, q)
	}
}

func (s *Server) authorizeWrite(next handler) handler {
	return func(w http.ResponseWriter, r *http.Request, q *request) *apiError {
		topics, _, e := s.topicsFromPath(r.URL.Path)
		if e != nil {
			return e
		}
		for _, t := range topics {
			if !s.allowed(q.token, t, true) {
				return errForbidden
			}
		}
		return next(w, r, q)
	}
}

func (s *Server) authorizeRead(next handler) handler {
	return func(w http.ResponseWriter, r *http.Request, q *request) *apiError {
		topics, _, e := s.topicsFromPath(r.URL.Path)
		if e != nil {
			return e
		}
		for _, t := range topics {
			if !s.allowed(q.token, t, false) {
				return errForbidden
			}
		}
		return next(w, r, q)
	}
}

// topicsFromPath returns the deduplicated topics of the first path segment and its raw text.
func (s *Server) topicsFromPath(path string) ([]string, string, *apiError) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return nil, "", errTopicInvalid
	}
	var topics []string
	for _, t := range strings.Split(parts[1], ",") {
		if t == "" || slices.Contains(topics, t) {
			continue
		}
		if !topicRegex.MatchString(t) {
			return nil, "", errTopicInvalid
		}
		if slices.Contains(disallowedTopics, t) {
			return nil, "", errTopicDisallowed
		}
		topics = append(topics, t)
	}
	if len(topics) == 0 {
		return nil, "", errTopicInvalid
	}
	if len(topics) > s.cfg.SubscribeTopicsLimit {
		return nil, "", errTooManyTopics
	}
	return topics, parts[1], nil
}

func writeJSON(w http.ResponseWriter, v any) *apiError {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return nil // The client went away; there is nobody to send an error to
	}
	return nil
}

func handleOptions(w http.ResponseWriter, _ *http.Request, _ *request) *apiError {
	h := w.Header()
	h.Set("Access-Control-Allow-Methods", "GET, PUT, POST, PATCH, DELETE")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Headers", "*")
	return nil
}

func handleTopicAuth(w http.ResponseWriter, _ *http.Request, _ *request) *apiError {
	return writeJSON(w, map[string]bool{"success": true})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request, _ *request) *apiError {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		slog.Error("health check failed", "err", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"healthy":false,"role":"active"}` + "\n"))
		return nil
	}
	return writeJSON(w, map[string]any{"healthy": true, "role": "active"})
}

func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request, _ *request) *apiError {
	return writeJSON(w, map[string]any{
		"base_url":            "",
		"app_root":            "/",
		"enable_login":        false,
		"require_login":       s.cfg.DefaultAccess == store.PermDeny,
		"enable_signup":       false,
		"enable_payments":     false,
		"enable_calls":        false,
		"enable_emails":       false,
		"enable_reservations": false,
		"enable_web_push":     false,
		"billing_contact":     "",
		"disallowed_topics":   disallowedTopics,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request, _ *request) *apiError {
	n, err := s.db.MessageCount()
	if err != nil {
		return errInternal
	}
	return writeJSON(w, map[string]int64{"messages": n})
}

// handleAccount answers just enough for clients that probe it. There are no accounts here.
func (s *Server) handleAccount(w http.ResponseWriter, _ *http.Request, q *request) *apiError {
	if q.token == nil {
		return writeJSON(w, map[string]string{"username": "*", "role": "anonymous"})
	}
	return writeJSON(w, map[string]string{"username": q.token.Name, "role": "user"})
}
