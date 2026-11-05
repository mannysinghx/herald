// Package model holds the wire types. The JSON shape matches the ntfy subscribe API so that
// existing ntfy clients (CLI, Android, iOS) can talk to this server.
package model

import (
	"crypto/rand"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Event names on the wire.
const (
	EventOpen          = "open"
	EventKeepalive     = "keepalive"
	EventMessage       = "message"
	EventMessageDelete = "message_delete"
	EventMessageClear  = "message_clear"
	EventPollRequest   = "poll_request"
)

const (
	idLength       = 12
	actionIDLength = 10
	alphabet       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// Message is one notification. Seq and Sender never leave the server.
type Message struct {
	ID          string      `json:"id"`
	SequenceID  string      `json:"sequence_id,omitempty"`
	Time        int64       `json:"time"`
	Expires     int64       `json:"expires,omitempty"`
	Event       string      `json:"event"`
	Topic       string      `json:"topic"`
	Title       string      `json:"title,omitempty"`
	Message     string      `json:"message,omitempty"`
	Priority    int         `json:"priority,omitempty"`
	Tags        []string    `json:"tags,omitempty"`
	Click       string      `json:"click,omitempty"`
	Icon        string      `json:"icon,omitempty"`
	Actions     []*Action   `json:"actions,omitempty"`
	Attachment  *Attachment `json:"attachment,omitempty"`
	PollID      string      `json:"poll_id,omitempty"`
	ContentType string      `json:"content_type,omitempty"`
	Encoding    string      `json:"encoding,omitempty"`

	Seq    int64  `json:"-"` // Delivery order; assigned when the message goes out, 0 if it was never stored
	Sender string `json:"-"` // Visitor key of the publisher; attachment traffic is charged to it
}

// Attachment describes a file on a message.
type Attachment struct {
	Name    string `json:"name"`
	Type    string `json:"type,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Expires int64  `json:"expires,omitempty"`
	URL     string `json:"url"`
}

// Action is a button on a notification.
type Action struct {
	ID      string            `json:"id"`
	Action  string            `json:"action"`
	Label   string            `json:"label"`
	Clear   bool              `json:"clear"`
	URL     string            `json:"url,omitempty"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	Intent  string            `json:"intent,omitempty"`
	Extras  map[string]string `json:"extras,omitempty"`
	Value   string            `json:"value,omitempty"`
}

// New returns a message stamped with a fresh ID and the current time.
func New(event, topic, body string) *Message {
	return &Message{ID: NewID(), Time: time.Now().Unix(), Event: event, Topic: topic, Message: body}
}

// ForJSON hides the sequence ID when it adds nothing, as the HTTP subscribe API does.
func (m *Message) ForJSON() *Message {
	if m.SequenceID != m.ID {
		return m
	}
	c := *m
	c.SequenceID = ""
	return &c
}

// IsNotification reports whether filters apply to this event.
func (m *Message) IsNotification() bool {
	return m.Event == EventMessage || m.Event == EventMessageDelete || m.Event == EventMessageClear
}

// NewID returns a random message ID.
func NewID() string { return RandomString(idLength) }

// ValidID reports whether s has the shape of a message ID.
func ValidID(s string) bool {
	if len(s) != idLength {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune(alphabet, c) {
			return false
		}
	}
	return true
}

// RandomString returns n unbiased random characters from the alphabet.
func RandomString(n int) string {
	out := make([]byte, 0, n)
	buf := make([]byte, n*2)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			panic(err) // crypto/rand does not fail on supported platforms
		}
		for _, b := range buf {
			if int(b) >= 256-(256%len(alphabet)) { // Reject to avoid modulo bias
				continue
			}
			out = append(out, alphabet[int(b)%len(alphabet)])
			if len(out) == n {
				break
			}
		}
	}
	return string(out)
}

// ParsePriority converts a priority string to 1-5, or 0 if empty.
func ParsePriority(s string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return 0, nil
	case "1", "min":
		return 1, nil
	case "2", "low":
		return 2, nil
	case "3", "default":
		return 3, nil
	case "4", "high":
		return 4, nil
	case "5", "max", "urgent":
		return 5, nil
	}
	return 0, errors.New("invalid priority: " + strconv.Quote(s))
}
