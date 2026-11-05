package server

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"herald/internal/model"
	"herald/internal/store"
)

// start runs a real server on a real socket, because the streaming behavior is the point.
func start(t *testing.T, change func(*Config)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.DefaultAccess = store.PermReadWrite
	cfg.RateLimitExempt = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	cfg.SchedulerInterval = 50 * time.Millisecond
	if change != nil {
		change(cfg)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { _ = s.Close() })
	return s, "http://" + l.Addr().String()
}

func publish(t *testing.T, base, topic, body string, headers ...string) (*model.Message, int) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/"+topic, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	m := &model.Message{}
	_ = json.NewDecoder(resp.Body).Decode(m)
	return m, resp.StatusCode
}

func poll(t *testing.T, base, path string, headers ...string) []*model.Message {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("poll %s: status %d", path, resp.StatusCode)
	}
	var out []*model.Message
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		m := &model.Message{}
		if err := json.Unmarshal(scanner.Bytes(), m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func bodies(messages []*model.Message) string {
	out := make([]string, 0, len(messages))
	for _, m := range messages {
		out = append(out, m.Message)
	}
	return strings.Join(out, ",")
}

// streamUntil reads a JSON stream, handing messages to f until it returns false.
func streamUntil(ctx context.Context, url string, f func(*model.Message) bool) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		m := &model.Message{}
		if err := json.Unmarshal(scanner.Bytes(), m); err != nil {
			return err
		}
		if !f(m) {
			return nil
		}
	}
	return scanner.Err()
}

// The central promise: a client that keeps reconnecting with since=<last id> while several
// publishers are running sees every message exactly once, in delivery order.
func TestReconnectingSubscriberSeesEveryMessageOnce(t *testing.T) {
	_, base := start(t, nil)
	const publishers, perPublisher = 4, 400
	total := publishers * perPublisher
	var wg sync.WaitGroup
	for p := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perPublisher {
				if _, code := publish(t, base, "reconnect", fmt.Sprintf("%d:%d", p, i)); code != 200 {
					t.Errorf("publish: status %d", code)
				}
			}
		}()
	}
	seen := map[string]int{}
	lastPerPublisher := map[int]int{}
	since := "all"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for received := 0; received < total && ctx.Err() == nil; {
		inThisConnection := 0
		err := streamUntil(ctx, base+"/reconnect/json?since="+since, func(m *model.Message) bool {
			if m.Event != model.EventMessage {
				return true
			}
			seen[m.ID]++
			var p, i int
			fmt.Sscanf(m.Message, "%d:%d", &p, &i)
			if prev, ok := lastPerPublisher[p]; ok && i != prev+1 {
				t.Errorf("publisher %d: got %d after %d", p, i, prev)
			}
			lastPerPublisher[p] = i
			since = m.ID
			received++
			inThisConnection++
			return inThisConnection < 37 && received < total // Hang up at awkward moments
		})
		if err != nil && ctx.Err() == nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("saw %d distinct messages, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s delivered %d times", id, n)
		}
	}
}

// Subscribing with since=all while messages are landing must not lose any in the gap between
// reading the backlog and going live, nor repeat any.
func TestSubscribeDuringPublishingHasNoGap(t *testing.T) {
	_, base := start(t, nil)
	const total = 600
	go func() {
		for i := range total {
			publish(t, base, "gap", fmt.Sprint(i))
		}
	}()
	time.Sleep(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	next := 0
	err := streamUntil(ctx, base+"/gap/json?since=all", func(m *model.Message) bool {
		if m.Event != model.EventMessage {
			return true
		}
		if m.Message != fmt.Sprint(next) {
			t.Fatalf("got %q, want %d", m.Message, next)
		}
		next++
		return next < total
	})
	if err != nil || next != total {
		t.Fatalf("got %d of %d: %v", next, total, err)
	}
}

// A scheduled message goes out after messages that were published later. A cursor on those must
// still find it, which is why order comes from delivery position and not from insertion.
func TestScheduledMessageIsNotHiddenBehindLaterCursor(t *testing.T) {
	_, base := start(t, func(c *Config) { c.MessageDelayMin = time.Second })
	if _, code := publish(t, base, "sched", "scheduled", "Delay", "1s"); code != 200 {
		t.Fatalf("status %d", code)
	}
	cursor, _ := publish(t, base, "sched", "published after")
	if got := bodies(poll(t, base, "/sched/json?poll=1")); got != "published after" {
		t.Fatalf("before due: %q", got)
	}
	var got []*model.Message
	deadline := time.Now().Add(5 * time.Second)
	for len(got) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		got = poll(t, base, "/sched/json?poll=1&since="+cursor.ID)
	}
	if bodies(got) != "scheduled" {
		t.Fatalf("after due, since cursor: %q", bodies(got))
	}
}

func TestScheduledMessageReachesLiveSubscriberOnce(t *testing.T) {
	_, base := start(t, func(c *Config) { c.MessageDelayMin = time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan string, 1)
	go streamUntil(ctx, base+"/live/json", func(m *model.Message) bool {
		if m.Event == model.EventOpen {
			if _, code := publish(t, base, "live", "soon", "In", "1s"); code != 200 {
				t.Errorf("status %d", code)
			}
			return true
		}
		if m.Event == model.EventMessage {
			got <- m.Message
			return false
		}
		return true
	})
	select {
	case body := <-got:
		if body != "soon" {
			t.Fatal(body)
		}
	case <-ctx.Done():
		t.Fatal("scheduled message never arrived")
	}
}

func TestSequenceReplacesPendingAndClearDoesNot(t *testing.T) {
	_, base := start(t, nil)
	publish(t, base, "p", "first", "In", "1h", "X-Sequence-ID", "s")
	publish(t, base, "p", "second", "In", "1h", "X-Sequence-ID", "s")
	if got := bodies(poll(t, base, "/p/json?poll=1&scheduled=1")); got != "second" {
		t.Fatalf("after replace: %q", got)
	}
	req, _ := http.NewRequest(http.MethodPut, base+"/p/s/clear", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var pending bool
	for _, m := range poll(t, base, "/p/json?poll=1&scheduled=1") {
		pending = pending || m.Message == "second"
	}
	if !pending {
		t.Fatal("clear must not drop a pending message")
	}
	req, _ = http.NewRequest(http.MethodDelete, base+"/p/s", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, m := range poll(t, base, "/p/json?poll=1&scheduled=1") {
		if m.Message == "second" {
			t.Fatal("delete must drop a pending message")
		}
	}
}

func TestMessagesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "r.db")
	s1, base1 := start(t, func(c *Config) { c.DBPath = dbPath })
	first, _ := publish(t, base1, "r", "one")
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	_, base2 := start(t, func(c *Config) { c.DBPath = dbPath })
	publish(t, base2, "r", "two")
	if got := bodies(poll(t, base2, "/r/json?poll=1&since="+first.ID)); got != "two" {
		t.Fatalf("since across restart: %q", got)
	}
	if got := bodies(poll(t, base2, "/r/json?poll=1")); got != "one,two" {
		t.Fatalf("all: %q", got)
	}
}

func TestExpiredMessagesArePruned(t *testing.T) {
	s, base := start(t, func(c *Config) { c.CacheDuration = time.Second })
	publish(t, base, "e", "old")
	if len(poll(t, base, "/e/json?poll=1")) != 1 {
		t.Fatal("message should still be there")
	}
	time.Sleep(1100 * time.Millisecond)
	if n := len(poll(t, base, "/e/json?poll=1")); n != 0 {
		t.Fatalf("expired message still served: %d", n)
	}
	ids, err := s.db.Prune(time.Now().Add(time.Hour))
	if err != nil || len(ids) != 1 {
		t.Fatalf("prune: %v %v", ids, err)
	}
}

func TestTruncatedReplayKeepsNewest(t *testing.T) {
	_, base := start(t, func(c *Config) { c.MessagePollSizeLimit = 800 })
	for i := range 20 {
		publish(t, base, "big", fmt.Sprintf("%d-%s", i, strings.Repeat("x", 100)))
	}
	resp, err := http.Get(base + "/big/json?poll=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.Header.Get("X-Messages-Truncated") != "1" {
		t.Fatal("missing truncation header")
	}
	if !strings.Contains(string(data), `"19-`) || strings.Contains(string(data), `"0-`) {
		t.Fatalf("should keep the newest: %s", data)
	}
}

func addToken(t *testing.T, s *Server, rules string) string {
	t.Helper()
	parsed, err := store.ParseRules(rules)
	if err != nil {
		t.Fatal(err)
	}
	_, secret, err := s.db.AddToken("t", parsed)
	if err != nil {
		t.Fatal(err)
	}
	return secret
}

func TestAccessControl(t *testing.T) {
	s, base := start(t, func(c *Config) { c.DefaultAccess = store.PermDeny })
	token := addToken(t, s, "alerts-*:rw,logs:ro,inbox:wo,alerts-secret:deny")
	bearer := []string{"Authorization", "Bearer " + token}
	cases := []struct {
		topic   string
		headers []string
		want    int
	}{
		{"alerts-a", nil, 403},
		{"alerts-a", bearer, 200},
		{"logs", bearer, 403},
		{"inbox", bearer, 200},
		{"other", bearer, 403},
		{"alerts-a", []string{"Authorization", "Bearer tk_wrong"}, 401},
		{"alerts-a", []string{"Authorization", "Basic " + base64.StdEncoding.EncodeToString([]byte("anyuser:"+token))}, 200},
		{"alerts-a", []string{"Authorization", "vapid x"}, 403},
	}
	for _, c := range cases {
		if _, code := publish(t, base, c.topic, "x", c.headers...); code != c.want {
			t.Errorf("publish %s %v: got %d, want %d", c.topic, c.headers, code, c.want)
		}
	}
	// First match wins, so a broad rule first would shadow this; document that it is order-based
	if _, code := publish(t, base, "alerts-secret", "x", bearer...); code != 200 {
		t.Errorf("rules should apply in the order written: %d", code)
	}
	auth := base64.RawURLEncoding.EncodeToString([]byte("Bearer " + token))
	resp, err := http.Get(base + "/alerts-a/json?poll=1&auth=" + auth)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("auth param: %d", resp.StatusCode)
	}
	for _, path := range []string{"/inbox/json?poll=1", "/alerts-a,logs2/json?poll=1"} {
		req, _ := http.NewRequest(http.MethodGet, base+path, nil)
		req.Header.Set(bearer[0], bearer[1])
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 403 {
			t.Errorf("%s: %d, want 403", path, resp.StatusCode)
		}
	}
}

func TestRevokedTokenLosesOpenStream(t *testing.T) {
	s, base := start(t, func(c *Config) {
		c.DefaultAccess = store.PermDeny
		c.KeepaliveInterval = 100 * time.Millisecond
	})
	token := addToken(t, s, "x:rw")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/x/json", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if n, err := s.db.RemoveToken("t"); n != 1 || err != nil {
		t.Fatalf("remove: %d %v", n, err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("stream should end cleanly once revoked: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("stream stayed open after revocation")
	}
}

func TestTokenIsStoredHashed(t *testing.T) {
	s, _ := start(t, nil)
	token := addToken(t, s, "x:rw")
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := os.ReadFile(s.cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	wal, _ := os.ReadFile(s.cfg.DBPath + "-wal")
	if strings.Contains(string(db)+string(wal), token) {
		t.Fatal("database contains the token secret")
	}
	reopened, err := store.Open(s.cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	s.db = reopened
}

func TestRateLimits(t *testing.T) {
	_, base := start(t, func(c *Config) {
		c.RateLimitExempt = nil
		c.RequestBurst = 3
		c.RequestReplenish = time.Hour
	})
	for i := range 3 {
		if _, code := publish(t, base, "rl", "x"); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	resp, err := http.Post(base+"/rl", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e apiError
	_ = json.NewDecoder(resp.Body).Decode(&e)
	if resp.StatusCode != 429 || e.Code != 42901 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d, code %d, retry %q", resp.StatusCode, e.Code, resp.Header.Get("Retry-After"))
	}
}

func TestDailyMessageLimit(t *testing.T) {
	_, base := start(t, func(c *Config) {
		c.RateLimitExempt = nil
		c.MessageDailyLimit = 2
	})
	publish(t, base, "d", "1")
	publish(t, base, "d", "2")
	if _, code := publish(t, base, "d", "3"); code != 429 {
		t.Fatalf("got %d", code)
	}
}

func TestSubscriptionLimit(t *testing.T) {
	_, base := start(t, func(c *Config) {
		c.RateLimitExempt = nil
		c.SubscriptionLimit = 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/s/json", nil)
	first, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	second, err := http.Get(base + "/s/json")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	if second.StatusCode != 429 {
		t.Fatalf("second subscription: %d", second.StatusCode)
	}
}

func TestAuthFailuresAreLimited(t *testing.T) {
	_, base := start(t, func(c *Config) {
		c.RateLimitExempt = nil
		c.AuthFailureBurst = 2
		c.DefaultAccess = store.PermDeny
	})
	codes := make([]int, 0, 3)
	for range 3 {
		_, code := publish(t, base, "a", "x", "Authorization", "Bearer tk_wrong")
		codes = append(codes, code)
	}
	if fmt.Sprint(codes) != "[401 401 429]" {
		t.Fatalf("got %v", codes)
	}
}

// A client must not choose who it is rate limited as by writing its own X-Forwarded-For.
func TestForwardedForTakesTheEntryOurProxyAdded(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	const xff = "X-Forwarded-For"
	if got := clientIP(r, true, xff).String(); got != "5.6.7.8" {
		t.Errorf("behind proxy: %s", got)
	}
	if got := clientIP(r, false, xff).String(); got != "10.0.0.1" {
		t.Errorf("not behind proxy, header must be ignored: %s", got)
	}
	r.Header.Set("X-Real-IP", "9.9.9.9")
	if got := clientIP(r, true, "X-Real-IP").String(); got != "9.9.9.9" {
		t.Errorf("configured header: %s", got)
	}
	r.Header.Set(xff, "garbage")
	if got := clientIP(r, true, xff).String(); got != "10.0.0.1" {
		t.Errorf("garbage header: %s", got)
	}
	six := netip.MustParseAddr("2001:db8:1:2:3:4:5:6")
	if ipKey(six) != ipKey(netip.MustParseAddr("2001:db8:1:2:ffff::1")) {
		t.Error("addresses in one /64 should share a visitor")
	}
}

// Requests through one proxy that names different clients must not share a rate limit, and the
// same client must keep one, whatever else it sends.
func TestRateLimitFollowsConfiguredProxyHeader(t *testing.T) {
	_, base := start(t, func(c *Config) {
		c.RateLimitExempt = nil
		c.RequestBurst = 2
		c.RequestReplenish = time.Hour
		c.BehindProxy = true
		c.ProxyHeader = "X-Real-IP"
	})
	send := func(realIP, forwarded string) int {
		_, code := publish(t, base, "p", "x", "X-Real-IP", realIP, "X-Forwarded-For", forwarded)
		return code
	}
	send("1.1.1.1", "7.7.7.1")
	send("1.1.1.1", "7.7.7.2")
	if code := send("1.1.1.1", "7.7.7.3"); code != 429 {
		t.Fatalf("a changing X-Forwarded-For should not escape the limit: %d", code)
	}
	if code := send("2.2.2.2", "7.7.7.3"); code != 200 {
		t.Fatalf("another client should have its own limit: %d", code)
	}
}

func attachmentServer(t *testing.T, change func(*Config)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	var base string
	s, b := start(t, func(c *Config) {
		c.AttachmentDir = dir
		c.BaseURL = "http://placeholder"
		if change != nil {
			change(c)
		}
	})
	base = b
	s.cfg.BaseURL = base
	return s, base
}

func TestAttachmentRoundTrip(t *testing.T) {
	_, base := attachmentServer(t, nil)
	data := strings.Repeat("\xff\x00", 3000)
	m, code := publish(t, base, "a", data)
	if code != 200 || m.Attachment == nil || m.Attachment.Size != 6000 {
		t.Fatalf("%d %+v", code, m.Attachment)
	}
	resp, err := http.Get(m.Attachment.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != data {
		t.Fatalf("got %d bytes", len(got))
	}
}

// An upload must not be able to run script on our origin.
func TestUploadedHTMLCannotRunAsPage(t *testing.T) {
	_, base := attachmentServer(t, nil)
	m, _ := publish(t, base, "a", "<html><script>alert(1)</script>", "Filename", "x.html")
	resp, err := http.Get(m.Attachment.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "html") {
		t.Errorf("content type: %s", ct)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing nosniff")
	}
	if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "sandbox") {
		t.Error("missing sandbox")
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment") {
		t.Error("should download, not display")
	}
}

func TestAttachmentLimitsCountActualBytes(t *testing.T) {
	s, base := attachmentServer(t, func(c *Config) { c.AttachmentFileLimit = 5000 })
	// No Content-Length: the limit must hold against what is really sent
	r, w := io.Pipe()
	go func() {
		_, _ = w.Write([]byte(strings.Repeat("x", 20000)))
		w.Close()
	}()
	req, _ := http.NewRequest(http.MethodPut, base+"/a", r)
	req.Header.Set("Filename", "big.bin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	entries, _ := os.ReadDir(s.cfg.AttachmentDir)
	if len(entries) != 0 {
		t.Fatalf("left %d files behind", len(entries))
	}
}

func TestAttachmentDownloadIsChargedToUploader(t *testing.T) {
	s, base := attachmentServer(t, func(c *Config) {
		c.BandwidthDailyLimit = 7000
		c.RateLimitExempt = nil
		c.RequestBurst = 100
	})
	m, _ := publish(t, base, "a", strings.Repeat("\xff", 6000))
	get := func() int {
		resp, err := http.Get(m.Attachment.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if code := get(); code != 200 {
		t.Fatalf("first: %d", code)
	}
	if code := get(); code != 429 {
		t.Fatalf("second: %d", code)
	}
	stored, err := s.db.Message(m.ID) // The sender is not part of the public JSON
	if err != nil || stored == nil {
		t.Fatal(stored, err)
	}
	uploader := s.visitors.get(stored.Sender)
	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	if uploader.bandwidth != 6000 {
		t.Fatalf("uploader charged %d", uploader.bandwidth)
	}
}

func TestAttachmentFilesAreCleanedUp(t *testing.T) {
	s, base := attachmentServer(t, nil)
	m, _ := publish(t, base, "a", strings.Repeat("\xff", 6000))
	path := filepath.Join(s.cfg.AttachmentDir, m.ID)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	ids, err := s.db.Prune(time.Now().Add(4 * time.Hour)) // After the attachment, before the message
	if err != nil {
		t.Fatal(err)
	}
	s.removeAttachments(ids)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expired attachment still on disk")
	}
	if len(poll(t, base, "/a/json?poll=1")) != 1 {
		t.Fatal("the message itself should outlive the attachment")
	}
	// An upload that no message row points to is swept by age
	stray := filepath.Join(s.cfg.AttachmentDir, "stray")
	if err := os.WriteFile(stray, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.sweepAttachments(time.Now())
	if _, err := os.Stat(stray); err != nil {
		t.Fatal("a fresh upload must not be swept")
	}
	s.sweepAttachments(time.Now().Add(5 * time.Hour))
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("stale upload was not swept")
	}
}

func TestUpstreamGetsOnlyHashAndID(t *testing.T) {
	type call struct{ path, pollID, auth, body string }
	calls := make(chan call, 4)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		calls <- call{r.URL.Path, r.Header.Get("X-Poll-ID"), r.Header.Get("Authorization"), string(data)}
	}))
	defer upstream.Close()
	_, base := start(t, func(c *Config) {
		c.BaseURL = "https://ntfy.example.com"
		c.UpstreamBaseURL = upstream.URL
		c.UpstreamToken = "tk_up"
	})
	m, _ := publish(t, base, "mytopic", "secret message", "Title", "secret title")
	select {
	case got := <-calls:
		want := fmt.Sprintf("/%x", sha256.Sum256([]byte("https://ntfy.example.com/mytopic")))
		if got.path != want || got.pollID != m.ID || got.auth != "Bearer tk_up" || got.body != "" {
			t.Fatalf("%+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no upstream call")
	}
	// Poll requests themselves and UnifiedPush messages must not be forwarded
	publish(t, base, "mytopic", "", "X-Poll-ID", "abc")
	publish(t, base, "mytopic?up=1", "up")
	select {
	case got := <-calls:
		t.Fatalf("unexpected forward: %+v", got)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestWebSocketDeliversAndAnswersPing(t *testing.T) {
	_, base := start(t, func(c *Config) { c.KeepaliveInterval = 100 * time.Millisecond })
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(base, "http")+"/w/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var pings atomic.Int32
	conn.SetPingHandler(func(string) error {
		pings.Add(1)
		return conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second))
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	m := &model.Message{}
	if err := conn.ReadJSON(m); err != nil || m.Event != model.EventOpen {
		t.Fatalf("%v %+v", err, m)
	}
	published, _ := publish(t, base, "w", "hello")
	if err := conn.ReadJSON(m); err != nil || m.ID != published.ID {
		t.Fatalf("%v %+v", err, m)
	}
	go func() { // Keep reading, which is what runs the ping handler
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	time.Sleep(350 * time.Millisecond)
	if pings.Load() == 0 {
		t.Fatal("no pings")
	}
}

// Closing the server must end streams instead of hanging on them.
func TestCloseEndsStreams(t *testing.T) {
	s, base := start(t, nil)
	resp, err := http.Get(base + "/c/json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Close hung")
	}
}

func TestParseFutureTime(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"30s", 30 * time.Second, true},
		{"10 min", 10 * time.Minute, true},
		{"2 hours", 2 * time.Hour, true},
		{"1d", 24 * time.Hour, true},
		{"1800000100", 100 * time.Second, true},
		{"2027-02-15T08:00:00Z", time.Date(2027, 2, 15, 8, 0, 0, 0, time.UTC).Sub(now), true},
		{"1000", 0, false},
		{"2020-01-15T08:00:00Z", 0, false},
		{"tomorrow", 0, false},
		{"!!!", 0, false},
	}
	for _, c := range cases {
		got, ok := parseFutureTime(c.in, now)
		if ok != c.ok || (ok && got.Sub(now) != c.want) {
			t.Errorf("%q: got %v %v", c.in, got.Sub(now), ok)
		}
	}
}
