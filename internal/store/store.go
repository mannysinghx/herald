// Package store persists messages and tokens in SQLite.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"herald/internal/model"

	_ "modernc.org/sqlite" // Pure Go driver, so the binary builds without cgo
)

const schema = `
CREATE TABLE IF NOT EXISTS messages (
	rowid_ INTEGER PRIMARY KEY AUTOINCREMENT,
	mid TEXT NOT NULL UNIQUE,
	topic TEXT NOT NULL,
	sequence_id TEXT NOT NULL,
	seq INTEGER UNIQUE,           -- Delivery order; NULL until a scheduled message goes out
	time INTEGER NOT NULL,
	expires INTEGER NOT NULL,
	sender TEXT NOT NULL,
	attachment_size INTEGER NOT NULL DEFAULT 0,
	attachment_expires INTEGER NOT NULL DEFAULT 0,
	attachment_deleted INTEGER NOT NULL DEFAULT 0,
	data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_topic_seq ON messages (topic, seq);
CREATE INDEX IF NOT EXISTS idx_messages_topic_time ON messages (topic, time);
CREATE INDEX IF NOT EXISTS idx_messages_due ON messages (time) WHERE seq IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages (expires);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages (sender) WHERE attachment_size > 0;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO meta (key, value) VALUES ('seq', 0), ('messages', 0);
CREATE TABLE IF NOT EXISTS tokens (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	hash TEXT NOT NULL UNIQUE,
	rules TEXT NOT NULL,
	created INTEGER NOT NULL
);
`

// Store is the database. Writes go through one connection, because SQLite has one writer anyway
// and queueing in Go is better than spinning on SQLITE_BUSY; reads use a pool thanks to WAL.
type Store struct {
	w, r *sql.DB
}

// Open opens or creates the database at path.
func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}
	r, err := sql.Open("sqlite", dsn+"&_pragma=query_only(ON)")
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(16)
	return &Store{w: w, r: r}, nil
}

// Close closes the database.
func (s *Store) Close() error { return errors.Join(s.r.Close(), s.w.Close()) }

// Insert stores a message. If publish is true it gets its delivery position now; otherwise it
// is scheduled and gets one in Publish. It sets m.Seq.
func (s *Store) Insert(m *model.Message, publish bool) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	var size, attachmentExpires int64
	if m.Attachment != nil && strings.Contains(m.Attachment.URL, "/file/"+m.ID) { // Only files we host
		size, attachmentExpires = m.Attachment.Size, m.Attachment.Expires
	}
	tx, err := s.w.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var seq sql.NullInt64
	if publish {
		if err := tx.QueryRow(`UPDATE meta SET value = value + 1 WHERE key = 'seq' RETURNING value`).Scan(&seq); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE meta SET value = value + 1 WHERE key = 'messages'`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO messages (mid, topic, sequence_id, seq, time, expires, sender, attachment_size, attachment_expires, data)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Topic, m.SequenceID, seq, m.Time, m.Expires, m.Sender, size, attachmentExpires, data); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.Seq = seq.Int64
	return nil
}

// Publish gives a scheduled message its delivery position. It returns false if the message was
// sent or deleted in the meantime.
func (s *Store) Publish(m *model.Message) (bool, error) {
	tx, err := s.w.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var seq int64
	if err := tx.QueryRow(`UPDATE meta SET value = value + 1 WHERE key = 'seq' RETURNING value`).Scan(&seq); err != nil {
		return false, err
	}
	res, err := tx.Exec(`UPDATE messages SET seq = ? WHERE mid = ? AND seq IS NULL`, seq, m.ID)
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	m.Seq = seq
	return true, nil
}

// Due returns the scheduled messages whose time has come.
func (s *Store) Due(now time.Time) ([]*model.Message, error) {
	return s.query(`SELECT data, seq, sender FROM messages WHERE seq IS NULL AND time <= ? ORDER BY time, rowid_`, now.Unix())
}

// SinceKind says where a replay starts.
type SinceKind int

// The replay starting points.
const (
	SinceNone SinceKind = iota
	SinceAll
	SinceLatest
	SinceTime
	SinceID
)

// Since is a replay starting point.
type Since struct {
	Kind SinceKind
	Time int64
	ID   string
}

// Replay returns the messages after since, in delivery order. Scheduled messages that are not
// due yet come last if scheduled is set. If the messages exceed maxBytes, the oldest are left
// out and truncated is true. high is the highest delivery position read, including messages the
// budget left out; anything at or below it is not news to the caller.
func (s *Store) Replay(topics []string, since Since, scheduled bool, maxBytes int) (messages []*model.Message, truncated bool, high int64, err error) {
	if since.Kind == SinceNone || len(topics) == 0 {
		return nil, false, 0, nil
	}
	if since.Kind == SinceLatest {
		return s.replayLatest(topics)
	}
	in := strings.TrimSuffix(strings.Repeat("?,", len(topics)), ",")
	args := make([]any, 0, len(topics)+2)
	for _, t := range topics {
		args = append(args, t)
	}
	now := time.Now().Unix()
	args = append(args, now)
	where := fmt.Sprintf(`topic IN (%s) AND (expires = 0 OR expires > ?) AND seq IS NOT NULL`, in)
	switch since.Kind {
	case SinceTime:
		where += ` AND time >= ?`
		args = append(args, since.Time)
	case SinceID:
		var seq sql.NullInt64
		err := s.r.QueryRow(`SELECT seq FROM messages WHERE mid = ?`, since.ID).Scan(&seq)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, false, 0, err
		}
		if seq.Valid { // An unknown or expired ID replays everything we still have; clients dedupe by ID
			where += ` AND seq > ?`
			args = append(args, seq.Int64)
		}
	}
	// Newest first, so that the size budget keeps the most recent messages. Streamed, so a topic
	// with a long backlog costs the budget in memory, not the backlog.
	budget := maxBytes
	err = s.each(func(m *model.Message, size int) bool {
		high = max(high, m.Seq)
		if budget -= size; budget < 0 {
			truncated = true
			return false
		}
		messages = append(messages, m)
		return true
	}, `SELECT data, seq, sender FROM messages WHERE `+where+` ORDER BY seq DESC`, args...)
	if err != nil {
		return nil, false, 0, err
	}
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
	if scheduled {
		pendingArgs := make([]any, 0, len(topics))
		for _, t := range topics {
			pendingArgs = append(pendingArgs, t)
		}
		pending, err := s.query(fmt.Sprintf(`SELECT data, seq, sender FROM messages WHERE topic IN (%s) AND seq IS NULL ORDER BY time, rowid_`, in), pendingArgs...)
		if err != nil {
			return nil, false, 0, err
		}
		messages = append(messages, pending...)
	}
	return messages, truncated, high, nil
}

func (s *Store) replayLatest(topics []string) ([]*model.Message, bool, int64, error) {
	var messages []*model.Message
	var high int64
	for _, t := range topics {
		rows, err := s.query(`SELECT data, seq, sender FROM messages WHERE topic = ? AND seq IS NOT NULL AND (expires = 0 OR expires > ?) ORDER BY seq DESC LIMIT 1`, t, time.Now().Unix())
		if err != nil {
			return nil, false, 0, err
		}
		for _, m := range rows {
			if m.Seq > high {
				high = m.Seq
			}
			messages = append(messages, m)
		}
	}
	return messages, false, high, nil
}

// DeleteScheduled removes not-yet-sent messages of a sequence and returns their IDs, whose
// attachments the caller should remove.
func (s *Store) DeleteScheduled(topic, sequenceID string) ([]string, error) {
	rows, err := s.w.Query(`DELETE FROM messages WHERE topic = ? AND sequence_id = ? AND seq IS NULL RETURNING mid`, topic, sequenceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Message returns one message by ID, or nil if there is none.
func (s *Store) Message(id string) (*model.Message, error) {
	rows, err := s.query(`SELECT data, seq, sender FROM messages WHERE mid = ?`, id)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// AttachmentBytes returns how much attachment storage a sender uses, or everyone if sender is "".
func (s *Store) AttachmentBytes(sender string) (int64, error) {
	q := `SELECT COALESCE(SUM(attachment_size), 0) FROM messages WHERE attachment_expires > ? AND attachment_deleted = 0`
	args := []any{time.Now().Unix()}
	if sender != "" {
		q += ` AND sender = ?`
		args = append(args, sender)
	}
	var n int64
	return n, s.r.QueryRow(q, args...).Scan(&n)
}

// Prune deletes expired messages and returns the IDs whose attachment files should be removed.
func (s *Store) Prune(now time.Time) ([]string, error) {
	var ids []string
	collect := func(q string) error {
		rows, err := s.w.Query(q, now.Unix())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	}
	if err := collect(`UPDATE messages SET attachment_deleted = 1 WHERE attachment_expires > 0 AND attachment_expires <= ? AND attachment_deleted = 0 RETURNING mid`); err != nil {
		return nil, err
	}
	if err := collect(`DELETE FROM messages WHERE expires > 0 AND expires <= ? RETURNING mid`); err != nil {
		return nil, err
	}
	return ids, nil
}

// MessageCount returns the number of messages ever accepted.
func (s *Store) MessageCount() (int64, error) {
	var n int64
	return n, s.r.QueryRow(`SELECT value FROM meta WHERE key = 'messages'`).Scan(&n)
}

// CountMessage counts a message that is not stored.
func (s *Store) CountMessage() error {
	_, err := s.w.Exec(`UPDATE meta SET value = value + 1 WHERE key = 'messages'`)
	return err
}

// Ping checks that the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.r.PingContext(ctx) }

func (s *Store) query(q string, args ...any) ([]*model.Message, error) {
	var out []*model.Message
	err := s.each(func(m *model.Message, _ int) bool {
		out = append(out, m)
		return true
	}, q, args...)
	return out, err
}

// each calls f for each row with the message and its stored size, until f returns false.
func (s *Store) each(f func(m *model.Message, size int) bool, q string, args ...any) error {
	rows, err := s.r.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var data, sender string
		var seq sql.NullInt64
		if err := rows.Scan(&data, &seq, &sender); err != nil {
			return err
		}
		m := &model.Message{}
		if err := json.Unmarshal([]byte(data), m); err != nil {
			return err
		}
		m.Seq, m.Sender = seq.Int64, sender
		if !f(m, len(data)) {
			return nil
		}
	}
	return rows.Err()
}
