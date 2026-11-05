package server

import (
	"math"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// visitor holds the limits of one client: a /32 or /64 network, or a token. IPv6 is keyed by
// /64 because a single host is commonly handed a whole /64 to hop around in.
type visitor struct {
	key      string
	requests *rate.Limiter
	auth     *rate.Limiter

	mu             sync.Mutex
	subscriptions  int
	messages       int
	messagesReset  time.Time
	bandwidth      int64
	bandwidthReset time.Time
	lastSeen       time.Time
}

func (v *visitor) allowRequest(exempt bool) *apiError {
	if exempt || v.requests.Allow() {
		return nil
	}
	return errLimitRequests.withRetryAfter(secondsUntilToken(v.requests))
}

func (v *visitor) allowMessage(exempt bool, limit int, now time.Time) *apiError {
	if exempt {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if now.After(v.messagesReset) {
		v.messages, v.messagesReset = 0, now.Add(24*time.Hour)
	}
	if v.messages >= limit {
		return errLimitMessages.withRetryAfter(int(v.messagesReset.Sub(now).Seconds()) + 1)
	}
	v.messages++
	return nil
}

func (v *visitor) addSubscription(exempt bool, limit int) *apiError {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !exempt && v.subscriptions >= limit {
		return errLimitSubscriptions
	}
	v.subscriptions++
	return nil
}

func (v *visitor) removeSubscription() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.subscriptions--
}

func (v *visitor) allowBandwidth(exempt bool, n, limit int64, now time.Time) *apiError {
	if exempt {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if now.After(v.bandwidthReset) {
		v.bandwidth, v.bandwidthReset = 0, now.Add(24*time.Hour)
	}
	if v.bandwidth+n > limit {
		return errLimitBandwidth.withRetryAfter(int(v.bandwidthReset.Sub(now).Seconds()) + 1)
	}
	v.bandwidth += n
	return nil
}

func secondsUntilToken(l *rate.Limiter) int {
	if limit := float64(l.Limit()); limit > 0 {
		return int(math.Ceil(1 / limit))
	}
	return 1
}

// visitors is the table of live visitors. Counters live in memory: a restart resets the limits,
// which errs on the side of letting notifications through.
type visitors struct {
	cfg *Config
	mu  sync.Mutex
	all map[string]*visitor
}

func newVisitors(cfg *Config) *visitors {
	return &visitors{cfg: cfg, all: map[string]*visitor{}}
}

// get returns the visitor for a key, creating it if needed.
func (t *visitors) get(key string) *visitor {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.all[key]
	if !ok {
		v = &visitor{
			key:      key,
			requests: rate.NewLimiter(rate.Every(t.cfg.RequestReplenish), t.cfg.RequestBurst),
			auth:     rate.NewLimiter(rate.Every(time.Minute), t.cfg.AuthFailureBurst),
		}
		t.all[key] = v
	}
	v.mu.Lock()
	v.lastSeen = time.Now()
	v.mu.Unlock()
	return v
}

// isExempt is asked per request, not stored on the visitor: visitors are shared by key, and an
// uploader or token must not inherit the exemption of whoever happened to touch it first.
func (t *visitors) isExempt(ip netip.Addr) bool {
	for _, p := range t.cfg.RateLimitExempt {
		if ip.IsValid() && p.Contains(ip) {
			return true
		}
	}
	return false
}

// prune forgets idle visitors. It waits out the daily counters, so an idle spell does not reset them.
func (t *visitors) prune(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key, v := range t.all {
		v.mu.Lock()
		idle := v.subscriptions == 0 && now.Sub(v.lastSeen) > 25*time.Hour
		v.mu.Unlock()
		if idle {
			delete(t.all, key)
		}
	}
}

// clientIP returns the address to rate limit by. Behind a proxy it reads the configured header and
// takes the right-most entry, the one the proxy added: everything to its left is whatever the
// client sent. Which header is right depends on the proxy. On Railway the right-most
// X-Forwarded-For entry is not the client, and X-Real-IP is.
func clientIP(r *http.Request, behindProxy bool, header string) netip.Addr {
	if behindProxy {
		parts := strings.Split(r.Header.Get(header), ",")
		for i := len(parts) - 1; i >= 0; i-- {
			if a, err := netip.ParseAddr(strings.TrimSpace(parts[i])); err == nil {
				return a.Unmap()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return a.Unmap()
}

func ipKey(ip netip.Addr) string {
	if !ip.IsValid() {
		return "ip:unknown"
	}
	if ip.Is6() {
		if p, err := ip.Prefix(64); err == nil {
			return "ip:" + p.String()
		}
	}
	return "ip:" + ip.String()
}
