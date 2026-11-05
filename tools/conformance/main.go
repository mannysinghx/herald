// Command conformance drives a server through a fixed scenario and prints a normalized transcript
// of what it answered. Random IDs and clocks are replaced, so transcripts of two servers can be
// diffed: run it against the reference ntfy, then against herald, and compare.
//
//	conformance -url http://127.0.0.1:28181 -scenario protocol > out.txt
//
// Scenarios: protocol (needs open access and no rate limits), limits (needs default limits),
// auth (needs deny-by-default and -token, see scripts/conformance.sh).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type runner struct {
	base   string
	client *http.Client
	ids    map[string]string // Real random ID -> stable placeholder
	last   string            // ID of the last publish, for since=<id>
}

type req struct {
	method  string
	path    string
	body    string
	headers map[string]string
}

var idRegex = regexp.MustCompile(`\b[a-zA-Z0-9]{12}\b`)

func main() {
	url := flag.String("url", "", "base URL of the server")
	scenario := flag.String("scenario", "protocol", "protocol, limits or auth")
	token := flag.String("token", "", "token with rw on alerts*, ro on readonly*, wo on writeonly* (auth scenario)")
	flag.Parse()
	r := &runner{base: strings.TrimRight(*url, "/"), client: &http.Client{Timeout: 30 * time.Second}, ids: map[string]string{}}
	switch *scenario {
	case "protocol":
		r.protocol()
	case "limits":
		r.limits()
	case "auth":
		r.auth(*token)
	default:
		fmt.Fprintln(os.Stderr, "unknown scenario")
		os.Exit(2)
	}
}

// normalize rewrites a JSON line to stable form; other text passes through with the base removed.
func (r *runner) normalize(line string) string {
	line = strings.ReplaceAll(line, r.base, "BASE")
	prefix := ""
	if strings.HasPrefix(line, "data: ") {
		prefix, line = "data: ", line[len("data: "):]
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(line), &v); err != nil {
		return prefix + r.scrubIDs(line)
	}
	delete(v, "link") // Links to documentation are the server's own business
	if id, ok := v["id"].(string); ok {
		v["id"] = r.placeholder(id)
		if seq, ok := v["sequence_id"].(string); ok && seq == id {
			v["sequence_id"] = v["id"]
		}
	}
	t, hasTime := v["time"].(float64)
	if hasTime {
		v["time"] = "T"
	}
	if e, ok := v["expires"].(float64); ok && hasTime {
		v["expires"] = fmt.Sprintf("T+%d", int64(e-t))
	}
	if a, ok := v["attachment"].(map[string]any); ok {
		if e, ok := a["expires"].(float64); ok && hasTime {
			a["expires"] = fmt.Sprintf("T+%d", int64(e-t))
		}
		if u, ok := a["url"].(string); ok {
			a["url"] = r.scrubIDs(u)
		}
	}
	if actions, ok := v["actions"].([]any); ok {
		for _, x := range actions {
			if a, ok := x.(map[string]any); ok {
				a["id"] = "ACTION"
			}
		}
	}
	out, _ := json.Marshal(v) // Sorted keys, so field order does not count as a difference
	return prefix + string(out)
}

func (r *runner) placeholder(id string) string {
	if p, ok := r.ids[id]; ok {
		return p
	}
	p := fmt.Sprintf("ID%d", len(r.ids)+1)
	r.ids[id] = p
	return p
}

func (r *runner) scrubIDs(s string) string {
	return idRegex.ReplaceAllStringFunc(s, func(m string) string {
		if p, ok := r.ids[m]; ok {
			return p
		}
		return m
	})
}

func (r *runner) do(q req) (*http.Response, []byte) {
	method := q.method
	if method == "" {
		method = http.MethodGet
		if q.body != "" {
			method = http.MethodPost
		}
	}
	request, err := http.NewRequest(method, r.base+q.path, strings.NewReader(q.body))
	if err != nil {
		panic(err)
	}
	for k, v := range q.headers {
		request.Header.Set(k, v)
	}
	resp, err := r.client.Do(request)
	if err != nil {
		fmt.Println("REQUEST FAILED:", strings.ReplaceAll(err.Error(), r.base, "BASE"))
		return nil, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, body
}

var reportedHeaders = []string{"Content-Type", "Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers", "Content-Disposition", "X-Messages-Truncated", "Retry-After"}

// step performs a request and prints its normalized result.
func (r *runner) step(name string, q req) []byte {
	fmt.Println("## " + name)
	resp, body := r.do(q)
	if resp == nil {
		return nil
	}
	fmt.Println("status:", resp.StatusCode)
	for _, h := range reportedHeaders {
		if v := resp.Header.Get(h); v != "" {
			if h == "Content-Disposition" {
				v = r.scrubIDs(v)
			}
			fmt.Printf("%s: %s\n", h, v)
		}
	}
	first := true
	for _, line := range strings.Split(strings.TrimRight(string(body), "\n"), "\n") {
		if line == "" && first && len(body) == 0 {
			break
		}
		first = false
		fmt.Println(r.normalize(line))
		var m struct {
			ID string `json:"id"`
		}
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) == nil && m.ID != "" {
			r.last = m.ID
		}
	}
	return body
}

func h(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// stream opens a streaming subscription, runs during, and prints what arrived.
func (r *runner) stream(name, path string, during func()) {
	fmt.Println("## " + name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.base+path, nil)
	resp, err := r.client.Do(request)
	if err != nil {
		fmt.Println("REQUEST FAILED")
		return
	}
	defer resp.Body.Close()
	fmt.Println("status:", resp.StatusCode)
	fmt.Println("Content-Type:", resp.Header.Get("Content-Type"))
	time.Sleep(300 * time.Millisecond)
	during()
	seen := newSeen()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if seen.first(scanner.Text()) {
			fmt.Println(r.normalize(scanner.Text()))
		}
	}
}

// seen hides repeats within one stream. ntfy delivers at least once: its background tail can send
// a message again that the subscriber just got from the replay, and clients dedupe by ID. Comparing
// what a client ends up showing keeps that timing out of the diff; herald's exactly-once behavior
// is asserted by its own tests.
type seenIDs map[string]bool

func newSeen() seenIDs { return seenIDs{} }

func (s seenIDs) first(line string) bool {
	var m struct {
		ID string `json:"id"`
	}
	if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil || m.ID == "" {
		return true
	}
	if s[m.ID] {
		return false
	}
	s[m.ID] = true
	return true
}

func (r *runner) websocket(name, path string, during func()) {
	fmt.Println("## " + name)
	url := "ws" + strings.TrimPrefix(r.base, "http") + path
	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		fmt.Println("dial failed, status:", code)
		return
	}
	defer conn.Close()
	time.Sleep(300 * time.Millisecond)
	during()
	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	seen := newSeen()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("(end)")
			return
		}
		if seen.first(string(data)) {
			fmt.Println(r.normalize(string(data)))
		}
	}
}

func (r *runner) publishQuietly(topic, body string, headers map[string]string) {
	resp, data := r.do(req{path: "/" + topic, body: body, headers: headers})
	if resp == nil || resp.StatusCode != 200 {
		fmt.Println("PUBLISH FAILED")
		return
	}
	var m struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(data, &m) == nil {
		r.last = m.ID
		r.placeholder(m.ID)
	}
}

func (r *runner) protocol() {
	// Publishing
	r.step("publish plain", req{path: "/t1", body: "hello world"})
	r.step("publish trims whitespace", req{path: "/t1", body: "  padded \n"})
	r.step("publish PUT", req{method: "PUT", path: "/t1", body: "via put"})
	r.step("publish empty body", req{method: "POST", path: "/t1"})
	r.step("publish all headers", req{path: "/t1", body: "b", headers: h("Title", "A title", "Priority", "5", "Tags", "warning, skull", "Click", "https://example.com/c", "Icon", "https://example.com/i.png")})
	r.step("publish X- headers", req{path: "/t1", body: "b", headers: h("X-Title", "XT", "X-Priority", "high", "X-Tags", "x")})
	r.step("publish short aliases", req{path: "/t1", body: "b", headers: h("t", "short", "p", "min", "ta", "s")})
	r.step("publish query params", req{method: "POST", path: "/t1?title=qt&priority=urgent&tags=a,b&message=qm"})
	r.step("publish header message newline", req{method: "POST", path: "/t1", headers: h("Message", `line1\nline2`)})
	r.step("publish encoded title", req{path: "/t1", body: "b", headers: h("Title", "=?UTF-8?B?8J+YgCBoaQ==?=")})
	r.step("publish RFC 9218 priority ignored", req{path: "/t1", body: "b", headers: h("Priority", "u=1, i")})
	r.step("publish markdown", req{path: "/t1", body: "**b**", headers: h("Markdown", "yes")})
	r.step("publish markdown content type", req{path: "/t1", body: "**b**", headers: h("Content-Type", "text/markdown")})
	r.step("publish via GET publish", req{path: "/t1/publish?message=g1&title=gt"})
	r.step("publish via GET send", req{path: "/t1/send?message=g2"})
	r.step("publish via GET trigger", req{path: "/t1/trigger"})
	r.step("publish JSON", req{path: "/", body: `{"topic":"t1","title":"jt","message":"jm","priority":2,"tags":["a","b"],"click":"https://example.com/j"}`})
	r.step("publish JSON minimal", req{path: "/", body: `{"topic":"t1"}`})
	r.step("publish JSON actions", req{path: "/", body: `{"topic":"t1","message":"ja","actions":[{"action":"view","label":"Open","url":"https://example.com"}]}`})
	r.step("publish JSON invalid", req{path: "/", body: `not json`})
	r.step("publish JSON bad topic", req{path: "/", body: `{"topic":"a b"}`})

	// Validation
	r.step("invalid priority", req{path: "/t1", body: "b", headers: h("Priority", "9")})
	r.step("title too long", req{path: "/t1", body: "b", headers: h("Title", strings.Repeat("x", 1025))})
	r.step("tags too long", req{path: "/t1", body: "b", headers: h("Tags", strings.Repeat("x", 1025))})
	r.step("icon invalid", req{path: "/t1", body: "b", headers: h("Icon", "ftp://x")})
	r.step("attach invalid", req{path: "/t1", body: "b", headers: h("Attach", "ftp://x")})
	r.step("email disabled", req{path: "/t1", body: "b", headers: h("Email", "a@example.com")})
	r.step("call disabled", req{path: "/t1", body: "b", headers: h("Call", "yes")})
	r.step("topic disallowed", req{path: "/v1", body: "b"})
	r.step("topic disallowed docs", req{path: "/docs", body: "b"})
	r.step("topic too long", req{path: "/" + strings.Repeat("a", 65), body: "b"})
	r.step("unknown path", req{path: "/v1/nope"})
	r.step("unknown method path", req{method: "POST", path: "/a/b/c/d", body: "b"})

	// Actions
	r.step("actions simple view", req{path: "/t2", body: "b", headers: h("Actions", "view, Open, https://example.com/a?x=1, clear=true")})
	r.step("actions simple multiple", req{path: "/t2", body: "b", headers: h("Actions", "view, A, https://example.com; http, B, https://example.com/b, method=PUT, headers.X-Y=z, body=hi; copy, C, val")})
	r.step("actions keyed", req{path: "/t2", body: "b", headers: h("Actions", "action=view, label=Open, url=https://example.com")})
	r.step("actions quoted", req{path: "/t2", body: "b", headers: h("Actions", `view, "Look, a comma", https://example.com`)})
	r.step("actions broadcast", req{path: "/t2", body: "b", headers: h("Actions", "broadcast, Go, extras.cmd=pic, intent=io.x.Y")})
	r.step("actions JSON header", req{path: "/t2", body: "b", headers: h("Actions", `[{"action":"copy","label":"C","value":"v"}]`)})
	r.step("actions too many", req{path: "/t2", body: "b", headers: h("Actions", "view, A, https://x.io; view, B, https://x.io; view, C, https://x.io; view, D, https://x.io")})
	r.step("actions unknown", req{path: "/t2", body: "b", headers: h("Actions", "nope, A, https://x.io")})
	r.step("actions missing url", req{path: "/t2", body: "b", headers: h("Actions", "view, A")})

	// Updating
	r.step("sequence via path", req{method: "PUT", path: "/t3/seq1", body: "v1"})
	r.step("sequence via path again", req{method: "POST", path: "/t3/seq1", body: "v2"})
	r.step("sequence via header", req{path: "/t3", body: "v3", headers: h("X-Sequence-ID", "seq2")})
	r.step("sequence invalid", req{path: "/t3", body: "v", headers: h("X-Sequence-ID", "a b")})
	r.step("clear via PUT", req{method: "PUT", path: "/t3/seq1/clear"})
	r.step("read via GET", req{path: "/t3/seq1/read"})
	r.step("delete via DELETE", req{method: "DELETE", path: "/t3/seq1"})
	r.step("delete via GET", req{path: "/t3/seq2/delete"})
	r.step("poll sees the events", req{path: "/t3/json?poll=1"})

	// Polling formats and filters
	r.step("poll json", req{path: "/t1/json?poll=1&priority=5"})
	r.step("poll sse", req{path: "/t1/sse?poll=1&title=jt"})
	r.step("poll raw", req{path: "/t1/raw?poll=1&tags=x"})
	r.step("poll raw flattens newlines", req{path: "/t1/raw?poll=1&message=line1%0Aline2"})
	r.step("filter default priority", req{path: "/t4/json?poll=1"})
	r.publishQuietly("t4", "pri-none", nil)
	r.publishQuietly("t4", "pri-3", h("Priority", "3"))
	r.publishQuietly("t4", "pri-4", h("Priority", "4", "Tags", "a,b"))
	r.step("filter priority 3 matches unset", req{path: "/t4/json?poll=1&priority=default"})
	r.step("filter priorities", req{path: "/t4/json?poll=1&priority=3,high"})
	r.step("filter priority invalid", req{path: "/t4/json?poll=1&priority=9"})
	r.step("filter all tags needed", req{path: "/t4/json?poll=1&tags=a,c"})
	r.step("filter tags", req{path: "/t4/json?poll=1&tags=b,a"})
	r.step("filter via header", req{path: "/t4/json?poll=1", headers: h("X-Message", "pri-3")})
	r.step("filter id", req{path: "/t4/json?poll=1&id=" + r.last})

	// Replay. Sleeps put each marker in its own second: see docs for the same-second difference.
	time.Sleep(1100 * time.Millisecond)
	r.publishQuietly("t5", "s1", nil)
	first := r.last
	time.Sleep(1100 * time.Millisecond)
	r.publishQuietly("t5", "s2", nil)
	second := r.last
	time.Sleep(1100 * time.Millisecond)
	r.publishQuietly("t5", "s3", nil)
	r.publishQuietly("t6", "other", nil)
	r.step("since all", req{path: "/t5/json?poll=1&since=all"})
	r.step("since none", req{path: "/t5/json?poll=1&since=none"})
	r.step("since latest", req{path: "/t5/json?poll=1&since=latest"})
	r.step("since first id", req{path: "/t5/json?poll=1&since=" + first})
	r.step("since second id", req{path: "/t5/json?poll=1&since=" + second})
	r.step("since unknown id", req{path: "/t5/json?poll=1&since=aaaaaaaaaaaa"})
	r.step("since far future time", req{path: "/t5/json?poll=1&since=4000000000"})
	r.step("since long duration", req{path: "/t5/json?poll=1&since=1h"})
	r.step("since invalid", req{path: "/t5/json?poll=1&since=nonsense"})
	r.step("since via header", req{path: "/t5/json?poll=1", headers: h("X-Since", second)})
	r.step("multiple topics", req{path: "/t5,t6/json?poll=1&since=" + second})
	r.step("duplicate topics", req{path: "/t6,t6/json?poll=1"})
	r.step("no cache publish", req{path: "/t7", body: "gone", headers: h("Cache", "no")})
	r.step("no cache is not replayed", req{path: "/t7/json?poll=1"})

	// Streams
	r.stream("stream json", "/s1/json", func() { r.publishQuietly("s1", "live", h("Title", "lt")) })
	r.stream("stream sse", "/s2/sse", func() { r.publishQuietly("s2", "live", nil) })
	r.stream("stream raw", "/s3/raw", func() { r.publishQuietly("s3", "live\nnewline", nil) })
	r.stream("stream multiple topics", "/s4,s5/json", func() {
		r.publishQuietly("s4", "to s4", nil)
		r.publishQuietly("s5", "to s5", nil)
		r.publishQuietly("s6", "not subscribed", nil)
	})
	r.stream("stream filter", "/s7/json?tags=keep", func() {
		r.publishQuietly("s7", "dropped", nil)
		r.publishQuietly("s7", "kept", h("Tags", "keep"))
	})
	r.publishQuietly("s8", "before", nil)
	r.stream("stream with since replays first", "/s8/json?since=all", func() { r.publishQuietly("s8", "after", nil) })
	r.stream("stream poll ends", "/s8/json?poll=1", func() {})
	r.websocket("websocket live", "/w1/ws", func() { r.publishQuietly("w1", "live", h("Title", "wt")) })
	r.publishQuietly("w2", "old", nil)
	r.websocket("websocket since all", "/w2/ws?since=all", func() { r.publishQuietly("w2", "new", nil) })
	r.websocket("websocket poll", "/w2/ws?poll=1", func() {})
	r.websocket("websocket filter", "/w3/ws?priority=5", func() {
		r.publishQuietly("w3", "no", nil)
		r.publishQuietly("w3", "yes", h("Priority", "5"))
	})
	r.step("websocket without upgrade", req{path: "/w1/ws"})
	r.websocket("websocket forbidden topic", "/v1/ws", func() {})

	// Attachments
	binary := string(bytes.Repeat([]byte{0xff, 0xfe, 0x00, 0x01}, 100))
	body := r.step("attachment by filename", req{method: "PUT", path: "/a1", body: "small text", headers: h("Filename", "note.txt")})
	r.download("download by filename", body)
	body = r.step("attachment binary body", req{method: "PUT", path: "/a1", body: binary})
	r.download("download binary", body)
	body = r.step("attachment oversized text", req{method: "PUT", path: "/a1", body: strings.Repeat("x", 4097)})
	r.download("download oversized", body)
	r.step("message at size limit", req{method: "PUT", path: "/a1", body: strings.Repeat("x", 4096)})
	body = r.step("attachment html", req{method: "PUT", path: "/a1", body: "<html><script>alert(1)</script>", headers: h("Filename", "x.html")})
	r.download("download html is not served as html", body)
	r.step("attachment external", req{path: "/a1", body: "see this", headers: h("Attach", "https://example.com/pic.png")})
	r.step("attachment external named", req{method: "POST", path: "/a1", headers: h("Attach", "https://example.com/pic.png", "Filename", "other.png")})
	r.step("attachment external no name", req{method: "POST", path: "/a1", headers: h("Attach", "https://example.com/")})
	r.step("unknown file", req{path: "/file/aaaaaaaaaaaa.bin"})
	r.step("attachment too large", req{method: "PUT", path: "/a2", body: strings.Repeat("x", 16*1024*1024)})
	r.step("poll attachments", req{path: "/a1/json?poll=1&title=none"})

	// Scheduled delivery
	r.step("delay too small", req{path: "/d1", body: "b", headers: h("In", "1s")})
	r.step("delay too large", req{path: "/d1", body: "b", headers: h("Delay", "4d")})
	r.step("delay unparseable", req{path: "/d1", body: "b", headers: h("At", "!!!")})
	r.step("delay without cache", req{path: "/d1", body: "b", headers: h("Delay", "1h", "Cache", "no")})
	r.step("delay duration", req{path: "/d1", body: "later", headers: h("Delay", "11s")})
	r.step("delay duration words", req{path: "/d2", body: "pending", headers: h("In", "2 hours")})
	r.step("delay is hidden from poll", req{path: "/d1/json?poll=1"})
	r.step("delay shows with scheduled", req{path: "/d1/json?poll=1&scheduled=1"})
	r.step("delay replaced by sequence", req{path: "/d2", body: "replaced", headers: h("In", "2 hours", "X-Sequence-ID", "sq")})
	r.step("delay replaced again", req{path: "/d2", body: "replaced twice", headers: h("In", "2 hours", "X-Sequence-ID", "sq")})
	r.step("delay shows replacement once", req{path: "/d2/json?poll=1&scheduled=1"})
	r.step("delete drops pending", req{method: "DELETE", path: "/d2/sq"})
	r.step("pending is gone", req{path: "/d2/json?poll=1&scheduled=1&message=replaced%20twice"})
	time.Sleep(22 * time.Second) // Reference sender ticks every 10s
	r.step("delay was delivered", req{path: "/d1/json?poll=1"})

	// Odds and ends
	r.step("options", req{method: "OPTIONS", path: "/t1"})
	r.step("health", req{path: "/v1/health"})
	r.step("topic auth", req{path: "/t1/auth"})
	r.step("unifiedpush discovery", req{path: "/t1?up=1"})
	r.step("unifiedpush binary", req{method: "PUT", path: "/up?up=1", body: "\xff\xfe"})
	r.step("unifiedpush text keeps space", req{method: "PUT", path: "/up?up=1", body: " keep "})
	r.step("poll request", req{method: "POST", path: "/p1", headers: h("X-Poll-ID", "abc123")})
	r.step("poll request not stored", req{path: "/p1/json?poll=1"})
	r.step("head root", req{method: "HEAD", path: "/"})
}

func (r *runner) download(name string, publishResponse []byte) {
	var m struct {
		Attachment struct {
			URL string `json:"url"`
		} `json:"attachment"`
	}
	fmt.Println("## " + name)
	if json.Unmarshal(publishResponse, &m) != nil || m.Attachment.URL == "" {
		fmt.Println("no attachment in response")
		return
	}
	path := strings.TrimPrefix(m.Attachment.URL, r.base)
	resp, body := r.do(req{path: path})
	if resp == nil {
		return
	}
	fmt.Println("status:", resp.StatusCode, "bytes:", len(body))
	fmt.Println("Content-Type:", resp.Header.Get("Content-Type"))
	fmt.Println("Content-Disposition:", resp.Header.Get("Content-Disposition"))
	head, _ := r.do(req{method: "HEAD", path: path})
	if head != nil {
		fmt.Println("head:", head.StatusCode, head.Header.Get("Content-Length"))
	}
}

func (r *runner) limits() {
	counts := map[string]int{}
	var limited *http.Response
	var body []byte
	for range 70 {
		resp, data := r.do(req{path: "/rl", body: "b"})
		if resp == nil {
			continue
		}
		counts[fmt.Sprint(resp.StatusCode)]++
		if resp.StatusCode == 429 && limited == nil {
			limited, body = resp, data
		}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("## request burst")
	for _, k := range keys {
		fmt.Printf("%s: %d\n", k, counts[k])
	}
	if limited != nil {
		fmt.Println(r.normalize(strings.TrimSpace(string(body))))
		fmt.Println("has Retry-After:", limited.Header.Get("Retry-After") != "")
	}
}

func (r *runner) auth(token string) {
	bearer := h("Authorization", "Bearer "+token)
	r.step("anonymous publish", req{path: "/alerts1", body: "b"})
	r.step("anonymous read", req{path: "/alerts1/json?poll=1"})
	r.step("anonymous auth probe", req{path: "/alerts1/auth"})
	r.step("anonymous health still works", req{path: "/v1/health"})
	r.step("bearer publish", req{path: "/alerts1", body: "secret", headers: bearer})
	r.step("bearer read", req{path: "/alerts1/json?poll=1", headers: bearer})
	r.step("bearer auth probe", req{path: "/alerts1/auth", headers: bearer})
	r.step("bearer on other topic", req{path: "/other", body: "b", headers: bearer})
	r.step("bearer read other topic", req{path: "/other/json?poll=1", headers: bearer})
	r.step("bearer multi topic with forbidden", req{path: "/alerts1,other/json?poll=1", headers: bearer})
	r.step("wrong bearer", req{path: "/alerts1", body: "b", headers: h("Authorization", "Bearer tk_wrongwrongwrongwrongwrongwrong")})
	r.step("wrong bearer on public path", req{path: "/v1/health", headers: h("Authorization", "Bearer tk_wrongwrongwrongwrongwrongwrong")})
	basic := base64.StdEncoding.EncodeToString([]byte(":" + token))
	r.step("basic with empty user", req{path: "/alerts1/json?poll=1", headers: h("Authorization", "Basic "+basic)})
	auth := base64.RawURLEncoding.EncodeToString([]byte("Bearer " + token))
	r.step("auth query param", req{path: "/alerts1/json?poll=1&auth=" + auth})
	r.step("other auth scheme ignored", req{path: "/alerts1", body: "b", headers: h("Authorization", "vapid t=x")})
	r.step("read-only can read", req{path: "/readonly1/json?poll=1", headers: bearer})
	r.step("read-only cannot write", req{path: "/readonly1", body: "b", headers: bearer})
	r.step("write-only can write", req{path: "/writeonly1", body: "b", headers: bearer})
	r.step("write-only cannot read", req{path: "/writeonly1/json?poll=1", headers: bearer})
	r.websocket("websocket with query auth", "/alerts1/ws?since=all&auth="+auth, func() {})
	r.websocket("websocket without auth", "/alerts1/ws", func() {})
}
