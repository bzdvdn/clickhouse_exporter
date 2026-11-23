package ch

import "testing"

func TestQuote(t *testing.T) {
	cases := map[string]string{
		"plain":      "'plain'",
		"it's":       `'it\'s'`,
		`back\slash`: `'back\\slash'`,
	}
	for in, want := range cases {
		if got := Quote(in); got != want {
			t.Errorf("Quote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuoteList(t *testing.T) {
	got := QuoteList([]string{"a", "b'c"})
	want := `'a', 'b\'c'`
	if got != want {
		t.Fatalf("QuoteList = %q, want %q", got, want)
	}
	if got := QuoteList(nil); got != "" {
		t.Fatalf("QuoteList(nil) = %q", got)
	}
}
