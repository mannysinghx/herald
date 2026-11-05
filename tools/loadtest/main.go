// Command loadtest holds open many subscriptions on one server, publishes messages at them, and
// reports publish throughput and end-to-end delivery latency. The server must exempt this client
// from rate limits, or the numbers measure the limiter.
//
//	loadtest -url http://127.0.0.1:28191 -subs 1000 -messages 2000 -publishers 8 -topics 10
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "", "base URL")
	subs := flag.Int("subs", 1000, "subscriptions to hold open")
	messages := flag.Int("messages", 2000, "messages to publish")
	publishers := flag.Int("publishers", 8, "concurrent publishers")
	topics := flag.Int("topics", 10, "topics to spread the subscriptions over")
	flag.Parse()

	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		MaxIdleConnsPerHost: *publishers * 2,
	}
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := func(i int) string { return fmt.Sprintf("load%s%d", run, i%*topics) }
	var ready, failed, received atomic.Int64
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, *messages)
	var firstFailure sync.Once
	fail := func(reason any) {
		failed.Add(1)
		firstFailure.Do(func() { fmt.Fprintln(os.Stderr, "first failure:", reason) })
	}
	connecting := make(chan struct{}, 64) // A burst of dials would only measure the listen backlog
	for i := range *subs {
		connecting <- struct{}{}
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, *url+"/"+topic(i)+"/json", nil)
			resp, err := client.Do(req)
			<-connecting
			if err != nil {
				fail(err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				fail(resp.Status)
				return
			}
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				var m struct {
					Event   string `json:"event"`
					Message string `json:"message"`
				}
				if json.Unmarshal(scanner.Bytes(), &m) != nil {
					continue
				}
				switch m.Event {
				case "open":
					ready.Add(1)
				case "message":
					received.Add(1)
					if sent, err := strconv.ParseInt(m.Message, 10, 64); err == nil {
						latency := time.Duration(time.Now().UnixNano() - sent)
						mu.Lock()
						latencies = append(latencies, latency)
						mu.Unlock()
					}
				}
			}
		}()
	}
	for ready.Load()+failed.Load() < int64(*subs) {
		time.Sleep(50 * time.Millisecond)
	}
	if failed.Load() > 0 {
		fmt.Fprintf(os.Stderr, "%d subscriptions failed\n", failed.Load())
		os.Exit(1)
	}
	fmt.Printf("subscriptions open: %d\n", ready.Load())

	var next, publishErrors atomic.Int64
	var wg sync.WaitGroup
	expected := int64(0)
	for i := range *messages { // Count who should receive what, given how subscriptions were spread
		perTopic := *subs / *topics
		if i%*topics < *subs%*topics {
			perTopic++
		}
		expected += int64(perTopic)
	}
	start := time.Now()
	for range *publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(*messages) {
					return
				}
				body := strconv.FormatInt(time.Now().UnixNano(), 10)
				resp, err := client.Post(*url+"/"+topic(int(i)), "text/plain", strings.NewReader(body))
				if err != nil {
					publishErrors.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					publishErrors.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	publishTime := time.Since(start)
	deadline := time.Now().Add(30 * time.Second)
	for received.Load() < expected && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[min(int(float64(len(latencies))*p), len(latencies)-1)].Round(time.Microsecond)
	}
	fmt.Printf("published: %d in %v (%.0f/s), errors: %d\n", *messages, publishTime.Round(time.Millisecond), float64(*messages)/publishTime.Seconds(), publishErrors.Load())
	fmt.Printf("deliveries: %d of %d expected (%.0f/s)\n", received.Load(), expected, float64(received.Load())/time.Since(start).Seconds())
	fmt.Printf("latency: p50 %v, p99 %v, max %v\n", pct(0.5), pct(0.99), pct(1))
	if received.Load() != expected {
		os.Exit(1)
	}
}
