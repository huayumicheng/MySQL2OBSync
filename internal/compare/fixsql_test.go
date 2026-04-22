package compare

import "testing"

func TestSanitizeFileName(t *testing.T) {
	got := sanitizeFileName("db.t1")
	if got != "db_t1" {
		t.Fatalf("unexpected: %s", got)
	}
	got = sanitizeFileName("a/b\\c:d")
	if got != "a_b_c_d" {
		t.Fatalf("unexpected: %s", got)
	}
}

func TestQuoteString(t *testing.T) {
	got := quoteString("a'b\\c\n")
	if got != "'a\\'b\\\\c\\n'" {
		t.Fatalf("unexpected: %s", got)
	}
}
