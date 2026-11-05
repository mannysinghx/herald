package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/mannysinghx/herald/internal/model"
)

// Permission is what a rule or the server default allows on a topic.
type Permission string

// The permissions.
const (
	PermReadWrite Permission = "rw"
	PermReadOnly  Permission = "ro"
	PermWriteOnly Permission = "wo"
	PermDeny      Permission = "deny"
)

// CanRead reports whether the permission allows subscribing.
func (p Permission) CanRead() bool { return p == PermReadWrite || p == PermReadOnly }

// CanWrite reports whether the permission allows publishing.
func (p Permission) CanWrite() bool { return p == PermReadWrite || p == PermWriteOnly }

// ParsePermission accepts the short form and ntfy's long names.
func ParsePermission(s string) (Permission, error) {
	switch strings.ToLower(s) {
	case "rw", "read-write":
		return PermReadWrite, nil
	case "ro", "read-only":
		return PermReadOnly, nil
	case "wo", "write-only":
		return PermWriteOnly, nil
	case "deny", "deny-all", "none":
		return PermDeny, nil
	}
	return "", fmt.Errorf("unknown permission %q (use rw, ro, wo or deny)", s)
}

// Rule grants a permission on the topics matching a glob, e.g. "alerts-*".
type Rule struct {
	Pattern string     `json:"pattern"`
	Perm    Permission `json:"perm"`
}

// Token is an API credential. Only its hash is stored, so a copy of the database grants nothing.
type Token struct {
	ID      string
	Name    string
	Rules   []Rule
	Created int64
}

// Permission returns the first matching rule's permission, and false if no rule matches.
func (t *Token) Permission(topic string) (Permission, bool) {
	for _, r := range t.Rules {
		if ok, err := path.Match(r.Pattern, topic); err == nil && ok {
			return r.Perm, true
		}
	}
	return PermDeny, false
}

// ParseRules parses "alerts-*:rw,ops:ro"; a rule without a permission means rw.
func ParseRules(s string) ([]Rule, error) {
	var rules []Rule
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pattern, permStr, hasPerm := strings.Cut(part, ":")
		perm := PermReadWrite
		if hasPerm {
			var err error
			if perm, err = ParsePermission(permStr); err != nil {
				return nil, err
			}
		}
		if _, err := path.Match(pattern, "x"); err != nil {
			return nil, fmt.Errorf("invalid topic pattern %q: %w", pattern, err)
		}
		rules = append(rules, Rule{Pattern: pattern, Perm: perm})
	}
	if len(rules) == 0 {
		return nil, errors.New("at least one rule is required")
	}
	return rules, nil
}

func hashToken(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// AddToken creates a token and returns it with its secret, which cannot be recovered later.
func (s *Store) AddToken(name string, rules []Rule) (*Token, string, error) {
	data, err := json.Marshal(rules)
	if err != nil {
		return nil, "", err
	}
	secret := "tk_" + model.RandomString(32)
	t := &Token{ID: model.RandomString(8), Name: name, Rules: rules, Created: time.Now().Unix()}
	_, err = s.w.Exec(`INSERT INTO tokens (id, name, hash, rules, created) VALUES (?, ?, ?, ?, ?)`, t.ID, t.Name, hashToken(secret), data, t.Created)
	if err != nil {
		return nil, "", err
	}
	return t, secret, nil
}

// TokenBySecret returns the token with this secret, or nil.
func (s *Store) TokenBySecret(secret string) (*Token, error) {
	return s.scanToken(s.r.QueryRow(`SELECT id, name, rules, created FROM tokens WHERE hash = ?`, hashToken(secret)))
}

// TokenByID returns a token by its public ID, or nil. A subscription uses it to notice revocation.
func (s *Store) TokenByID(id string) (*Token, error) {
	return s.scanToken(s.r.QueryRow(`SELECT id, name, rules, created FROM tokens WHERE id = ?`, id))
}

func (s *Store) scanToken(row *sql.Row) (*Token, error) {
	t := &Token{}
	var rules string
	if err := row.Scan(&t.ID, &t.Name, &rules, &t.Created); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return t, json.Unmarshal([]byte(rules), &t.Rules)
}

// Tokens lists all tokens.
func (s *Store) Tokens() ([]*Token, error) {
	rows, err := s.r.Query(`SELECT id, name, rules, created FROM tokens ORDER BY created, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Token
	for rows.Next() {
		t := &Token{}
		var rules string
		if err := rows.Scan(&t.ID, &t.Name, &rules, &t.Created); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(rules), &t.Rules); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RemoveToken deletes a token by ID or name and returns how many it removed.
func (s *Store) RemoveToken(idOrName string) (int64, error) {
	res, err := s.w.Exec(`DELETE FROM tokens WHERE id = ? OR name = ?`, idOrName, idOrName)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
