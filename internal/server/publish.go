package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gabriel-vasile/mimetype"

	"herald/internal/model"
)

const (
	publishReadTimeout = 2 * time.Minute // Long enough for the largest upload, short enough to shed a stalled sender

	emptyMessageBody = "triggered"
	attachmentBody   = "You received a file: %s"
)

var sequenceIDRegex = regexp.MustCompile(`^[-_A-Za-z0-9]{1,64}$`)

type publishOptions struct {
	cache       bool
	unifiedPush bool
	delayed     bool
}

// handlePublish takes a message as body plus headers or query parameters.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(publishReadTimeout))
	topics, _, e := s.topicsFromPath(r.URL.Path)
	if e != nil {
		return e
	}
	m := model.New(model.EventMessage, topics[0], "")
	opts, e := s.parsePublishParams(r, m)
	if e != nil {
		return e
	}
	if e := q.visitor.allowMessage(q.exempt, s.cfg.MessageDailyLimit, time.Now()); e != nil {
		return e
	}
	m.Sender = q.visitor.key
	if opts.cache {
		m.Expires = m.Time + int64(s.cfg.CacheDuration.Seconds())
	}
	if pollID := param(r, "x-poll-id", "poll-id"); pollID != "" {
		id, at := m.ID, m.Time
		*m = *model.New(model.EventPollRequest, topics[0], "New message")
		m.ID, m.Time, m.PollID, m.Sender = id, at, pollID, q.visitor.key
		opts.cache = false
		_, _ = io.Copy(io.Discard, r.Body)
	} else if e := s.readBody(r, q, m, opts); e != nil {
		return e
	}
	if m.Message == "" {
		m.Message = emptyMessageBody
	}
	if opts.cache && m.SequenceID != m.ID {
		if e := s.dropPending(m.Topic, m.SequenceID); e != nil { // A newer version replaces a pending one
			return e
		}
	}
	if e := s.deliver(m, opts); e != nil {
		return e
	}
	return writeJSON(w, m.ForJSON())
}

// dropPending removes the scheduled messages of a sequence, with their attachments.
func (s *Server) dropPending(topic, sequenceID string) *apiError {
	ids, err := s.db.DeleteScheduled(topic, sequenceID)
	if err != nil {
		slog.Error("deleting scheduled messages failed", "err", err)
		return errInternal
	}
	s.removeAttachments(ids)
	return nil
}

// handlePublishJSON takes {"topic":..., "message":...} on "/", for webhook senders that can only
// post JSON. It rewrites the request into the header form, so there is one code path.
func (s *Server) handlePublishJSON(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	var in struct {
		Topic      string          `json:"topic"`
		Title      string          `json:"title"`
		Message    string          `json:"message"`
		Priority   int             `json:"priority"`
		Tags       []string        `json:"tags"`
		Click      string          `json:"click"`
		Icon       string          `json:"icon"`
		Actions    json.RawMessage `json:"actions"`
		Attach     string          `json:"attach"`
		Filename   string          `json:"filename"`
		Markdown   bool            `json:"markdown"`
		Email      string          `json:"email"`
		Call       string          `json:"call"`
		Cache      string          `json:"cache"`
		Firebase   string          `json:"firebase"`
		Delay      string          `json:"delay"`
		SequenceID string          `json:"sequence_id"`
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(publishReadTimeout))
	limit := int64(s.cfg.MessageSizeLimit) * 2 // JSON adds overhead around the message
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return errJSONInvalid
	}
	if int64(len(body)) > limit {
		return errJSONTooLarge
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return errJSONInvalid
	}
	if !topicRegex.MatchString(in.Topic) {
		return errTopicInvalid
	}
	if in.Message == "" {
		in.Message = emptyMessageBody
	}
	r.URL.Path = "/" + in.Topic
	r.Body = io.NopCloser(strings.NewReader(in.Message))
	set := func(header, value string) {
		if value != "" {
			r.Header.Set(header, value)
		}
	}
	set("X-Title", in.Title)
	if in.Priority != 0 {
		set("X-Priority", strconv.Itoa(in.Priority))
	}
	set("X-Tags", strings.Join(in.Tags, ","))
	set("X-Click", in.Click)
	set("X-Icon", in.Icon)
	set("X-Attach", in.Attach)
	set("X-Filename", in.Filename)
	set("X-Email", in.Email)
	set("X-Call", in.Call)
	set("X-Cache", in.Cache)
	set("X-Firebase", in.Firebase)
	set("X-Delay", in.Delay)
	set("X-Sequence-ID", in.SequenceID)
	if in.Markdown {
		set("X-Markdown", "yes")
	}
	if len(in.Actions) > 0 && string(in.Actions) != "null" {
		set("X-Actions", string(in.Actions))
	}
	return s.authorizeWrite(s.handlePublish)(w, r, q)
}

func (s *Server) parsePublishParams(r *http.Request, m *model.Message) (publishOptions, *apiError) {
	opts := publishOptions{}
	if r.Method != http.MethodGet && updatePathRegex.MatchString(r.URL.Path) {
		m.SequenceID = strings.Split(r.URL.Path, "/")[2]
	} else if sid := param(r, "x-sequence-id", "sequence-id", "sid"); sid != "" {
		if !sequenceIDRegex.MatchString(sid) {
			return opts, errSequenceIDInvalid
		}
		m.SequenceID = sid
	} else {
		m.SequenceID = m.ID
	}
	opts.cache = boolParam(r, true, "x-cache", "cache")
	m.Title = param(r, "x-title", "title", "t")
	if len(m.Title) > s.cfg.TitleSizeLimit {
		return opts, errTitleTooLarge
	}
	m.Click = param(r, "x-click", "click")
	filename := param(r, "x-filename", "filename", "file", "f")
	attach := param(r, "x-attach", "attach", "a")
	if attach != "" || filename != "" {
		m.Attachment = &model.Attachment{Name: filename}
	}
	if attach != "" {
		if !urlRegex.MatchString(attach) {
			return opts, errAttachURLInvalid
		}
		m.Attachment.URL = attach
		if m.Attachment.Name == "" {
			if u, err := url.Parse(attach); err == nil {
				if base := path.Base(u.Path); base != "." && base != "/" {
					m.Attachment.Name = base
				}
			}
		}
		if m.Attachment.Name == "" {
			m.Attachment.Name = "attachment"
		}
	}
	if icon := param(r, "x-icon", "icon"); icon != "" {
		if !urlRegex.MatchString(icon) {
			return opts, errIconURLInvalid
		}
		m.Icon = icon
	}
	// These channels are not built yet. Say so rather than dropping the request on the floor.
	if param(r, "x-email", "x-e-mail", "email", "e-mail", "mail", "e") != "" {
		return opts, errEmailDisabled
	}
	if param(r, "x-call", "call") != "" {
		return opts, errCallsDisabled
	}
	if param(r, "x-template", "template", "tpl") != "" {
		return opts, errTemplateInvalid
	}
	if body := param(r, "x-message", "message", "m"); body != "" {
		m.Message = strings.ReplaceAll(body, `\n`, "\n")
	}
	var err error
	if m.Priority, err = model.ParsePriority(param(r, "x-priority", "priority", "prio", "p")); err != nil {
		return opts, errPriorityInvalid
	}
	m.Tags = commaParam(r, "x-tags", "tags", "tag", "ta")
	size := 0
	for _, tag := range m.Tags {
		size += len(tag)
	}
	if size > s.cfg.TagsSizeLimit {
		return opts, errTagsTooLarge
	}
	if delay := param(r, "x-delay", "delay", "x-at", "at", "x-in", "in"); delay != "" {
		if !opts.cache {
			return opts, errDelayNoCache
		}
		now := time.Now()
		at, ok := parseFutureTime(delay, now)
		switch {
		case !ok:
			return opts, errDelayCannotParse
		case at.Unix() < now.Add(s.cfg.MessageDelayMin).Unix():
			return opts, errDelayTooSmall
		case at.Unix() > now.Add(s.cfg.MessageDelayMax).Unix():
			return opts, errDelayTooLarge
		}
		m.Time = at.Unix()
		opts.delayed = true
	}
	if actions := param(r, "x-actions", "actions", "action"); actions != "" {
		parsed, err := model.ParseActions(actions)
		if err != nil {
			return opts, errActionsInvalid.withDetail("%s", err.Error())
		}
		m.Actions = parsed
	}
	contentType := r.Header.Get("Content-Type")
	if boolParam(r, false, "x-markdown", "markdown", "md") || strings.HasPrefix(contentType, "text/markdown") {
		m.ContentType = "text/markdown"
	}
	opts.unifiedPush = boolParam(r, false, "x-unifiedpush", "unifiedpush", "up") || r.Header.Get("Content-Encoding") == "aes128gcm"
	return opts, nil
}

// readBody decides whether the body is the message or an attachment.
//
//   - UnifiedPush: the message, base64 if binary, not trimmed.
//   - An external attachment URL: the body is the message.
//   - A filename: the body is the attachment.
//   - Small valid UTF-8: the message.
//   - Anything else: an attachment.
func (s *Server) readBody(r *http.Request, q *request, m *model.Message, opts publishOptions) *apiError {
	peek := make([]byte, s.cfg.MessageSizeLimit)
	n, err := io.ReadFull(r.Body, peek)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return errInternal
	}
	peek = peek[:n]
	over := n == s.cfg.MessageSizeLimit // As in ntfy: a body of exactly the limit counts as over it
	switch {
	case opts.unifiedPush:
		if utf8.Valid(peek) {
			m.Message = string(peek)
		} else {
			m.Message = base64.StdEncoding.EncodeToString(peek)
			m.Encoding = "base64"
		}
		return nil
	case m.Attachment != nil && m.Attachment.URL != "":
		return setTextBody(m, peek)
	case m.Attachment != nil && m.Attachment.Name != "":
		return s.saveAttachment(r, q, m, peek)
	case !over && utf8.Valid(peek):
		return setTextBody(m, peek)
	}
	return s.saveAttachment(r, q, m, peek)
}

func setTextBody(m *model.Message, body []byte) *apiError {
	if !utf8.Valid(body) {
		return errNotUTF8
	}
	if len(body) > 0 { // An empty body must not override ?message= on a GET publish
		m.Message = strings.TrimSpace(string(body))
	}
	if m.Attachment != nil && m.Attachment.Name != "" && m.Message == "" {
		m.Message = fmt.Sprintf(attachmentBody, m.Attachment.Name)
	}
	return nil
}

// saveAttachment writes the file with the limits enforced on the bytes actually read, since a
// Content-Length is only a claim.
func (s *Server) saveAttachment(r *http.Request, q *request, m *model.Message, peek []byte) *apiError {
	if s.cfg.AttachmentDir == "" || s.cfg.BaseURL == "" {
		return errAttachDisallowed
	}
	expires := time.Now().Add(s.cfg.AttachmentExpiry).Unix()
	if m.Expires > 0 && expires > m.Expires {
		expires = m.Expires // An attachment must not outlive its message
	}
	if m.Time > expires {
		return errAttachBeforeSend
	}
	used, err := s.db.AttachmentBytes(q.visitor.key)
	if err != nil {
		return errInternal
	}
	total, err := s.db.AttachmentBytes("")
	if err != nil {
		return errInternal
	}
	limit := min(s.cfg.AttachmentFileLimit, s.cfg.AttachmentVisitorLimit-used, s.cfg.AttachmentTotalLimit-total)
	if limit <= 0 || r.ContentLength > limit {
		return errAttachTooLarge
	}
	if m.Attachment == nil {
		m.Attachment = &model.Attachment{}
	}
	contentType, ext := detectContentType(peek, m.Attachment.Name)
	tmp, err := os.CreateTemp(s.cfg.AttachmentDir, ".upload-*")
	if err != nil {
		return errInternal
	}
	defer os.Remove(tmp.Name()) // Fails harmlessly after the rename
	defer tmp.Close()
	written, err := io.Copy(tmp, io.LimitReader(io.MultiReader(bytes.NewReader(peek), r.Body), limit+1))
	if err != nil {
		return errInternal
	}
	if written > limit {
		return errAttachTooLarge
	}
	if err := tmp.Close(); err != nil {
		return errInternal
	}
	if err := os.Rename(tmp.Name(), filepath.Join(s.cfg.AttachmentDir, m.ID)); err != nil {
		return errInternal
	}
	m.Attachment.Size = written
	m.Attachment.Type = contentType
	m.Attachment.Expires = expires
	m.Attachment.URL = fmt.Sprintf("%s/file/%s%s", s.cfg.BaseURL, m.ID, ext)
	if m.Attachment.Name == "" {
		m.Attachment.Name = "attachment" + ext
	}
	if m.Message == "" {
		m.Message = fmt.Sprintf(attachmentBody, m.Attachment.Name)
	}
	return nil
}

func detectContentType(peek []byte, name string) (string, string) {
	if strings.HasSuffix(strings.ToLower(name), ".apk") {
		return "application/vnd.android.package-archive", ".apk" // Detected as a plain zip otherwise
	}
	m := mimetype.Detect(peek)
	if m.Extension() == "" {
		return m.String(), ".bin"
	}
	return m.String(), m.Extension()
}

// sweepAttachments removes files older than any attachment may live. That catches what Prune
// cannot: uploads whose message was never stored ("Cache: no"), or a crash before the insert.
func (s *Server) sweepAttachments(now time.Time) {
	if s.cfg.AttachmentDir == "" {
		return
	}
	entries, err := os.ReadDir(s.cfg.AttachmentDir)
	if err != nil {
		slog.Warn("reading attachment directory failed", "err", err)
		return
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || now.Sub(info.ModTime()) < s.cfg.AttachmentExpiry+time.Hour {
			continue
		}
		if err := os.Remove(filepath.Join(s.cfg.AttachmentDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("removing stale attachment failed", "name", entry.Name(), "err", err)
		}
	}
}

func (s *Server) removeAttachments(ids []string) {
	if s.cfg.AttachmentDir == "" {
		return
	}
	for _, id := range ids {
		if err := os.Remove(filepath.Join(s.cfg.AttachmentDir, id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("removing attachment failed", "id", id, "err", err)
		}
	}
}

// deliver persists a message, then fans it out, in that order: a subscriber that reconnects right
// after receiving a message can ask for what came after it only if the message is already stored.
func (s *Server) deliver(m *model.Message, opts publishOptions) *apiError {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	var err error
	switch {
	case opts.delayed:
		return s.errIf(s.db.Insert(m, false))
	case opts.cache:
		err = s.db.Insert(m, true)
	default:
		err = s.db.CountMessage()
	}
	if err != nil {
		return s.errIf(err)
	}
	s.hub.Publish(m)
	if !opts.unifiedPush {
		s.forwardUpstream(m)
	}
	return nil
}

func (s *Server) errIf(err error) *apiError {
	if err != nil {
		slog.Error("storing message failed", "err", err)
		return errInternal
	}
	return nil
}

// sendDue publishes the scheduled messages whose time has come.
func (s *Server) sendDue() {
	due, err := s.db.Due(time.Now())
	if err != nil {
		slog.Error("reading scheduled messages failed", "err", err)
		return
	}
	for _, m := range due {
		s.pubMu.Lock()
		ok, err := s.db.Publish(m)
		if err != nil {
			slog.Error("publishing scheduled message failed", "id", m.ID, "err", err)
		} else if ok {
			s.hub.Publish(m)
			s.forwardUpstream(m)
		}
		s.pubMu.Unlock()
	}
}

// handleAction publishes a message_delete or message_clear for a sequence ID. These are events
// in the stream, not row deletions: offline clients need to see them on replay.
func (s *Server) handleAction(event string) handler {
	return func(w http.ResponseWriter, r *http.Request, q *request) *apiError {
		topics, _, e := s.topicsFromPath(r.URL.Path)
		if e != nil {
			return e
		}
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 || !sequenceIDRegex.MatchString(parts[2]) {
			return errSequenceIDInvalid
		}
		if e := q.visitor.allowMessage(q.exempt, s.cfg.MessageDailyLimit, time.Now()); e != nil {
			return e
		}
		m := model.New(event, topics[0], "")
		m.SequenceID = parts[2]
		m.Sender = q.visitor.key
		m.Expires = m.Time + int64(s.cfg.CacheDuration.Seconds())
		if event == model.EventMessageDelete {
			if e := s.dropPending(m.Topic, m.SequenceID); e != nil {
				return e
			}
		}
		if e := s.deliver(m, publishOptions{cache: true, unifiedPush: true}); e != nil { // Not forwarded: nothing for iOS to fetch
			return e
		}
		return writeJSON(w, m.ForJSON())
	}
}

// handleFile serves an attachment. Its download traffic counts against the uploader, so that one
// upload shared with a thousand people cannot be used to burn other people's quota.
func (s *Server) handleFile(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	id := fileRegex.FindStringSubmatch(r.URL.Path)[1]
	f, err := os.Open(filepath.Join(s.cfg.AttachmentDir, id))
	if err != nil {
		return errNotFound
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return errNotFound
	}
	m, err := s.db.Message(id)
	if err != nil {
		return errInternal
	}
	if m == nil || m.Attachment == nil {
		return errNotFound
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	if r.Method == http.MethodHead {
		return nil
	}
	uploader := s.visitors.get(m.Sender)
	if e := uploader.allowBandwidth(q.exempt, info.Size(), s.cfg.BandwidthDailyLimit, time.Now()); e != nil {
		h.Del("Content-Length")
		return e
	}
	// Uploads are untrusted and served from our own origin: never let them run as a page there.
	h.Set("Content-Type", safeContentType(m.Attachment.Type))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	if m.Attachment.Name != "" {
		h.Set("Content-Disposition", "attachment; filename="+strconv.Quote(m.Attachment.Name))
	}
	_, _ = io.Copy(w, f)
	return nil
}

func safeContentType(t string) string {
	lower := strings.ToLower(t)
	if strings.Contains(lower, "html") || strings.Contains(lower, "xml") || strings.Contains(lower, "svg") || strings.Contains(lower, "javascript") {
		return "text/plain; charset=utf-8"
	}
	if t == "" {
		return "application/octet-stream"
	}
	return t
}

// forwardUpstream tells an upstream server that a message exists, sending only its ID and a hash
// of the topic URL. That lets the upstream wake an iOS device, which then fetches the message
// from us. The caller holds pubMu, so the work happens in the background.
func (s *Server) forwardUpstream(m *model.Message) {
	if s.cfg.UpstreamBaseURL == "" || m.Event != model.EventMessage || m.PollID != "" {
		return
	}
	select {
	case s.upstream <- struct{}{}:
	default:
		slog.Warn("upstream forwarding is saturated, dropping poll request", "id", m.ID)
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.upstream }()
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		defer cancel()
		hash := sha256.Sum256([]byte(s.cfg.BaseURL + "/" + m.Topic))
		target := fmt.Sprintf("%s/%x", strings.TrimRight(s.cfg.UpstreamBaseURL, "/"), hash)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
		if err != nil {
			slog.Warn("creating poll request failed", "err", err)
			return
		}
		req.Header.Set("X-Poll-ID", m.ID)
		if s.cfg.UpstreamToken != "" {
			req.Header.Set("Authorization", "Bearer "+s.cfg.UpstreamToken)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			slog.Warn("forwarding poll request failed", "err", err)
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			slog.Warn("upstream rejected poll request", "status", resp.Status)
		}
	}()
}
