package server

import (
	"mime"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"herald/internal/model"
	"herald/internal/store"
)

var (
	mimeDecoder mime.WordDecoder

	// Browsers and Cloudflare send "Priority: u=1, i" (RFC 9218), which is not a message priority.
	rfc9218Priority = regexp.MustCompile(`^u=\d,\s*(i|\d)$|^u=\d$`)
	durationRegex   = regexp.MustCompile(`(?i)^(\d+)\s*(d|days?|h|hours?|m|mins?|minutes?|s|secs?|seconds?)$`)
)

// param returns the first of the names set as a header, then as a query parameter. Every publish
// option can be given either way, so that webhooks which cannot set headers still work.
func param(r *http.Request, names ...string) string {
	for _, name := range names {
		value := r.Header.Get(name)
		if decoded, err := mimeDecoder.DecodeHeader(value); err == nil {
			value = decoded
		}
		if strings.EqualFold(name, "priority") && rfc9218Priority.MatchString(value) {
			value = ""
		}
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	query := r.URL.Query()
	for _, name := range names {
		if value := strings.TrimSpace(query.Get(strings.ToLower(name))); value != "" {
			return value
		}
	}
	return ""
}

func boolParam(r *http.Request, def bool, names ...string) bool {
	switch strings.ToLower(param(r, names...)) {
	case "":
		return def
	case "1", "yes", "true":
		return true
	}
	return false
}

func commaParam(r *http.Request, names ...string) []string {
	var out []string
	for _, part := range strings.Split(param(r, names...), ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// parseFutureTime reads a delay: a unix timestamp, a duration ("30m", "2 days") or RFC 3339.
func parseFutureTime(s string, now time.Time) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), n >= now.Unix()
	}
	if d, ok := parseDuration(s); ok {
		return now.Add(d), true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, t.After(now)
	}
	return time.Time{}, false
}

func parseDuration(s string) (time.Duration, bool) {
	m := durationRegex.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	unit := map[byte]time.Duration{'d': 24 * time.Hour, 'h': time.Hour, 'm': time.Minute, 's': time.Second}
	return time.Duration(n) * unit[strings.ToLower(m[2])[0]], true
}

func parseSince(r *http.Request, poll bool) (store.Since, *apiError) {
	s := param(r, "x-since", "since", "si")
	switch s {
	case "":
		if poll {
			return store.Since{Kind: store.SinceAll}, nil
		}
		return store.Since{Kind: store.SinceNone}, nil
	case "all":
		return store.Since{Kind: store.SinceAll}, nil
	case "latest":
		return store.Since{Kind: store.SinceLatest}, nil
	case "none":
		return store.Since{Kind: store.SinceNone}, nil
	}
	if model.ValidID(s) {
		return store.Since{Kind: store.SinceID, ID: s}, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return store.Since{Kind: store.SinceTime, Time: n}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return store.Since{Kind: store.SinceTime, Time: time.Now().Add(-d).Unix()}, nil
	}
	return store.Since{}, errSinceInvalid
}

// filter narrows a subscription; it applies to notifications only, not to open or keepalive.
type filter struct {
	id, message, title string
	tags               []string
	priorities         []int
}

func parseFilter(r *http.Request) (*filter, *apiError) {
	f := &filter{
		id:      param(r, "x-id", "id"),
		message: param(r, "x-message", "message", "m"),
		title:   param(r, "x-title", "title", "t"),
		tags:    commaParam(r, "x-tags", "tags", "tag", "ta"),
	}
	for _, p := range commaParam(r, "x-priority", "priority", "prio", "p") {
		n, err := model.ParsePriority(p)
		if err != nil {
			return nil, errPriorityInvalid
		}
		f.priorities = append(f.priorities, n)
	}
	return f, nil
}

func (f *filter) pass(m *model.Message) bool {
	if !m.IsNotification() {
		return true
	}
	if (f.id != "" && m.ID != f.id) || (f.message != "" && m.Message != f.message) || (f.title != "" && m.Title != f.title) {
		return false
	}
	if len(f.priorities) > 0 {
		priority := m.Priority
		if priority == 0 {
			priority = 3 // "Not set" means default priority
		}
		if !slices.Contains(f.priorities, priority) {
			return false
		}
	}
	for _, tag := range f.tags {
		if !slices.Contains(m.Tags, tag) {
			return false
		}
	}
	return true
}
