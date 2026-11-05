// Command herald is a self-hosted notification service: publish over HTTP, subscribe over JSON
// streams, SSE or WebSocket. It speaks the ntfy wire protocol, so ntfy clients can use it.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"herald/internal/server"
	"herald/internal/store"
)

func main() {
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve(args)
	case "token":
		err = tokenCommand(args)
	default:
		err = fmt.Errorf("unknown command %q (use serve or token)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func serve(args []string) error {
	cfg := server.DefaultConfig()
	listen := cfg.Listen
	if port := os.Getenv("PORT"); port != "" { // What Railway and most hosts set
		listen = ":" + port
	}
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.StringVar(&cfg.Listen, "listen", env("HERALD_LISTEN", listen), "address to listen on")
	fs.StringVar(&cfg.BaseURL, "base-url", env("HERALD_BASE_URL", ""), "public URL of this server; required for attachments and iOS")
	fs.StringVar(&cfg.DBPath, "db", env("HERALD_DB", cfg.DBPath), "SQLite database file")
	fs.StringVar(&cfg.AttachmentDir, "attachment-dir", env("HERALD_ATTACHMENT_DIR", ""), "directory for attachments; empty disables them")
	access := fs.String("default-access", env("HERALD_DEFAULT_ACCESS", "deny"), "access without a matching token rule: deny, ro, wo or rw")
	fs.BoolVar(&cfg.BehindProxy, "behind-proxy", env("HERALD_BEHIND_PROXY", "") == "true", "rate limit by the address in -proxy-header; only set this behind a proxy that writes it")
	fs.StringVar(&cfg.ProxyHeader, "proxy-header", env("HERALD_PROXY_HEADER", cfg.ProxyHeader), "header holding the client address when behind a proxy; X-Real-IP on Railway")
	fs.StringVar(&cfg.UpstreamBaseURL, "upstream-base-url", env("HERALD_UPSTREAM_BASE_URL", ""), "server to forward poll requests to, to wake iOS devices")
	fs.StringVar(&cfg.UpstreamToken, "upstream-token", env("HERALD_UPSTREAM_TOKEN", ""), "access token for the upstream server")
	exempt := fs.String("rate-limit-exempt", env("HERALD_RATE_LIMIT_EXEMPT", ""), "comma-separated CIDRs without rate limits")
	cacheDuration := fs.Duration("cache-duration", envDuration("HERALD_CACHE_DURATION", cfg.CacheDuration), "how long messages stay available for replay")
	keepalive := fs.Duration("keepalive", envDuration("HERALD_KEEPALIVE", cfg.KeepaliveInterval), "interval between keepalives on idle streams")
	verbose := fs.Bool("verbose", env("HERALD_VERBOSE", "") == "true", "log at debug level")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	perm, err := store.ParsePermission(*access)
	if err != nil {
		return err
	}
	cfg.DefaultAccess = perm
	cfg.CacheDuration = *cacheDuration
	cfg.KeepaliveInterval = *keepalive
	if cfg.AttachmentDir != "" && cfg.BaseURL == "" {
		return fmt.Errorf("-attachment-dir needs -base-url, to build download links")
	}
	if cfg.UpstreamBaseURL != "" && cfg.BaseURL == "" {
		return fmt.Errorf("-upstream-base-url needs -base-url, to hash the topic URL")
	}
	for _, part := range strings.Split(*exempt, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		p, err := netip.ParsePrefix(part)
		if err != nil {
			return fmt.Errorf("invalid -rate-limit-exempt entry %q: %w", part, err)
		}
		cfg.RateLimitExempt = append(cfg.RateLimitExempt, p)
	}
	s, err := server.New(cfg)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		_ = s.Close()
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	errs := make(chan error, 1)
	go func() { errs <- s.Serve(l) }()
	slog.Info("listening", "addr", l.Addr().String(), "default_access", string(perm), "attachments", cfg.AttachmentDir != "", "upstream", cfg.UpstreamBaseURL)
	select {
	case err := <-errs:
		return err
	case sig := <-stop:
		slog.Info("shutting down", "signal", sig.String())
		return s.Close()
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		slog.Warn("ignoring invalid duration", "key", key, "value", v)
	}
	return def
}

func tokenCommand(args []string) error {
	usage := fmt.Errorf("usage: herald token add -name NAME -allow 'alerts-*:rw,ops:ro' | list | rm NAME_OR_ID")
	if len(args) == 0 {
		return usage
	}
	fs := flag.NewFlagSet("token", flag.ContinueOnError)
	dbPath := fs.String("db", env("HERALD_DB", server.DefaultConfig().DBPath), "SQLite database file")
	name := fs.String("name", "", "a name to recognize the token by")
	allow := fs.String("allow", "", "comma-separated rules, pattern:perm, where perm is rw, ro, wo or deny")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	db, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "add":
		if *name == "" {
			return fmt.Errorf("-name is required")
		}
		rules, err := store.ParseRules(*allow)
		if err != nil {
			return err
		}
		t, secret, err := db.AddToken(*name, rules)
		if err != nil {
			return err
		}
		fmt.Printf("token %s (%s): %s\nStore it now: only its hash is kept.\n", t.ID, t.Name, secret)
	case "list":
		tokens, err := db.Tokens()
		if err != nil {
			return err
		}
		for _, t := range tokens {
			rules := make([]string, 0, len(t.Rules))
			for _, r := range t.Rules {
				rules = append(rules, r.Pattern+":"+string(r.Perm))
			}
			fmt.Printf("%s\t%s\t%s\t%s\n", t.ID, t.Name, time.Unix(t.Created, 0).Format(time.DateOnly), strings.Join(rules, ","))
		}
	case "rm":
		if fs.NArg() != 1 {
			return usage
		}
		n, err := db.RemoveToken(fs.Arg(0))
		if err != nil {
			return err
		}
		fmt.Println("removed " + strconv.FormatInt(n, 10))
	default:
		return usage
	}
	return nil
}
