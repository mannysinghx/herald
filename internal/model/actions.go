package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const maxActions = 3

// ParseActions reads action buttons from either a JSON array or the simple format:
//
//	view, Open, https://example.com, clear=true; http, Close, https://example.com/x, method=PUT, headers.A=b
func ParseActions(s string) ([]*Action, error) {
	s = strings.TrimSpace(s)
	var actions []*Action
	var err error
	if strings.HasPrefix(s, "[") {
		err = json.Unmarshal([]byte(s), &actions)
	} else {
		actions, err = parseSimpleActions(s)
	}
	if err != nil {
		return nil, err
	}
	if len(actions) > maxActions {
		return nil, fmt.Errorf("only %d actions allowed", maxActions)
	}
	for _, a := range actions {
		if a == nil {
			return nil, errors.New("action cannot be null")
		}
		a.ID = RandomString(actionIDLength)
		a.Action = strings.ToLower(a.Action)
		a.Method = strings.ToUpper(a.Method)
		switch {
		case !slices.Contains([]string{"view", "http", "broadcast", "copy"}, a.Action):
			return nil, fmt.Errorf("unknown action %q", a.Action)
		case a.Label == "":
			return nil, errors.New("parameter 'label' is required")
		case (a.Action == "view" || a.Action == "http") && a.URL == "":
			return nil, fmt.Errorf("parameter 'url' is required for action '%s'", a.Action)
		case a.Action == "copy" && a.Value == "":
			return nil, errors.New("parameter 'value' is required for action 'copy'")
		case a.Action == "http" && (a.Method == "GET" || a.Method == "HEAD") && a.Body != "":
			return nil, fmt.Errorf("parameter 'body' cannot be set if method is %s", a.Method)
		}
	}
	return actions, nil
}

// parseSimpleActions splits on ";" and "," outside of quotes, then assigns the parts: bare
// values fill action, label and the action's main value in order; key=value parts set fields.
func parseSimpleActions(s string) ([]*Action, error) {
	groups, err := tokenize(s)
	if err != nil {
		return nil, err
	}
	actions := make([]*Action, 0, len(groups))
	for _, parts := range groups {
		a := &Action{}
		position := 0
		for _, p := range parts {
			if p.key != "" {
				if err := setActionField(a, p.key, p.value); err != nil {
					return nil, err
				}
				continue
			}
			switch position {
			case 0:
				a.Action = p.value
			case 1:
				a.Label = p.value
			case 2:
				if a.Action == "copy" {
					a.Value = p.value
				} else if a.Action == "view" || a.Action == "http" {
					a.URL = p.value
				} else {
					return nil, fmt.Errorf("term '%s' unknown", p.value)
				}
			default:
				return nil, fmt.Errorf("term '%s' unknown", p.value)
			}
			position++
		}
		actions = append(actions, a)
	}
	return actions, nil
}

func setActionField(a *Action, key, value string) error {
	lower := strings.ToLower(key)
	switch {
	case lower == "action":
		a.Action = value
	case lower == "label":
		a.Label = value
	case lower == "url":
		a.URL = value
	case lower == "value":
		a.Value = value
	case lower == "method":
		a.Method = value
	case lower == "body":
		a.Body = value
	case lower == "intent":
		a.Intent = value
	case lower == "clear":
		switch strings.ToLower(value) {
		case "true", "yes", "1":
			a.Clear = true
		case "false", "no", "0":
			a.Clear = false
		default:
			return fmt.Errorf("invalid value %q for 'clear'", value)
		}
	case strings.HasPrefix(lower, "headers."):
		if a.Headers == nil {
			a.Headers = map[string]string{}
		}
		a.Headers[key[len("headers."):]] = value
	case strings.HasPrefix(lower, "extras."):
		if a.Extras == nil {
			a.Extras = map[string]string{}
		}
		a.Extras[key[len("extras."):]] = value
	default:
		return fmt.Errorf("unknown key %q", key)
	}
	return nil
}

type actionPart struct{ key, value string }

// tokenize scans the simple format into per-action parts. Quotes may wrap a whole value or the
// part after "key=", and hide separators; a backslash escapes the quote inside them.
func tokenize(s string) ([][]actionPart, error) {
	var groups [][]actionPart
	var group []actionPart
	var cur strings.Builder
	var key string
	var quote rune
	sawValue := false
	flush := func() {
		v := strings.TrimSpace(cur.String())
		if sawValue {
			v = cur.String() // Quoted: keep it exactly as written
		}
		if v != "" || key != "" || sawValue {
			group = append(group, actionPart{key: key, value: v})
		}
		cur.Reset()
		key, sawValue = "", false
	}
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case quote != 0:
			if c == '\\' && i+1 < len(runes) && runes[i+1] == quote {
				cur.WriteRune(quote)
				i++
			} else if c == quote {
				quote = 0
			} else {
				cur.WriteRune(c)
			}
		case (c == '"' || c == '\'') && strings.TrimSpace(cur.String()) == "":
			cur.Reset()
			quote, sawValue = c, true
		case c == '=' && key == "" && !sawValue && isActionKey(cur.String()):
			key = strings.TrimSpace(cur.String())
			cur.Reset()
		case c == ',':
			flush()
		case c == ';':
			flush()
			if len(group) > 0 {
				groups = append(groups, group)
			}
			group = nil
		default:
			cur.WriteRune(c)
		}
	}
	if quote != 0 {
		return nil, errors.New("unterminated quote")
	}
	flush()
	if len(group) > 0 {
		groups = append(groups, group)
	}
	return groups, nil
}

// isActionKey keeps URLs with query strings ("https://x?a=b") from being read as key=value.
func isActionKey(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	for _, c := range s {
		ok := c == '-' || c == '.' || c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !ok {
			return false
		}
	}
	return true
}
