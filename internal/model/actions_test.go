package model

import "testing"

func TestParseActionsSimple(t *testing.T) {
	actions, err := ParseActions(`view, "Look, a comma", https://x.io/a?b=c, clear=true; http, Close, https://x.io, method=put, headers.X-Y=z, body='a; b'; copy, Copy, v`)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 3 {
		t.Fatalf("got %d actions", len(actions))
	}
	view, http, copyAction := actions[0], actions[1], actions[2]
	if view.Label != "Look, a comma" || view.URL != "https://x.io/a?b=c" || !view.Clear {
		t.Errorf("%+v", view)
	}
	if http.Method != "PUT" || http.Headers["X-Y"] != "z" || http.Body != "a; b" {
		t.Errorf("%+v", http)
	}
	if copyAction.Value != "v" {
		t.Errorf("%+v", copyAction)
	}
	broadcast, err := ParseActions("broadcast, Go, extras.cmd=pic, intent=io.x.Y")
	if err != nil || broadcast[0].Extras["cmd"] != "pic" || broadcast[0].Intent != "io.x.Y" {
		t.Errorf("%+v %v", broadcast, err)
	}
	if view.ID == "" || view.ID == http.ID {
		t.Error("actions need distinct IDs")
	}
}

func TestParseActionsRejects(t *testing.T) {
	for _, in := range []string{
		"nope, A, https://x.io",
		"view, A",
		"view",
		"copy, A",
		"view, A, https://x.io, nope=1",
		"view, A, https://x.io, clear=maybe",
		`view, "unterminated, https://x.io`,
		"http, A, https://x.io, method=GET, body=x",
		"view, A, https://x.io; view, B, https://x.io; view, C, https://x.io; view, D, https://x.io",
		`[{"action":"view"}]`,
		`[null]`,
		`[not json`,
	} {
		if actions, err := ParseActions(in); err == nil {
			t.Errorf("%q should fail, got %+v", in, actions)
		}
	}
}

func TestRandomStringIsValidID(t *testing.T) {
	seen := map[string]bool{}
	for range 2000 {
		id := NewID()
		if !ValidID(id) {
			t.Fatalf("invalid ID %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = true
	}
	for _, bad := range []string{"", "short", "has-a-dash-1", "waytoolongforanid"} {
		if ValidID(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
}
