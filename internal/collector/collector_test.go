package collector

import "testing"

func TestDeltaTracker(t *testing.T) {
	d := newDeltaTracker()
	if got := d.delta("x", 10); got != 10 {
		t.Fatalf("first delta = %v, want 10", got)
	}
	if got := d.delta("x", 15); got != 5 {
		t.Fatalf("second delta = %v, want 5", got)
	}
	// Counter reset: export the new value.
	if got := d.delta("x", 3); got != 3 {
		t.Fatalf("reset delta = %v, want 3", got)
	}
}

func TestNotIn(t *testing.T) {
	if got := notIn("user", nil); got != "" {
		t.Fatalf("empty notIn = %q", got)
	}
	got := notIn("user", []string{"a", "b'c"})
	want := ` AND user NOT IN ('a', 'b\'c')`
	if got != want {
		t.Fatalf("notIn = %q, want %q", got, want)
	}
}

func TestReduceTableRows(t *testing.T) {
	rows := []tableRow{
		{Database: "db", Table: "a", Parts: 1, Rows: 10, DiskBytes: 100, UncompressedBytes: 200, LastModificationSeconds: 5},
		{Database: "db", Table: "b", Parts: 2, Rows: 20, DiskBytes: 200, UncompressedBytes: 400, LastModificationSeconds: 9},
		{Database: "db", Table: "c", Parts: 3, Rows: 30, DiskBytes: 300, UncompressedBytes: 600, LastModificationSeconds: 7},
	}
	kept, other := reduceTableRows(rows, 2)
	if len(kept) != 2 {
		t.Fatalf("kept = %d", len(kept))
	}
	if other == nil {
		t.Fatal("expected an other bucket")
		return
	}
	if other.Table != "__other__" || other.Database != "__other__" {
		t.Fatalf("other labels = %s.%s", other.Database, other.Table)
	}
	if other.Parts != 3 || other.Rows != 30 || other.DiskBytes != 300 {
		t.Fatalf("other sums = %+v", other)
	}
	if other.LastModificationSeconds != 7 {
		t.Fatalf("other last modification = %d", other.LastModificationSeconds)
	}
}

func TestReduceTableRowsNoOther(t *testing.T) {
	rows := []tableRow{{Table: "a"}, {Table: "b"}}
	kept, other := reduceTableRows(rows, 2)
	if len(kept) != 2 || other != nil {
		t.Fatalf("kept=%d other=%v", len(kept), other)
	}
}

func TestReduceWriteRows(t *testing.T) {
	rows := []writeRow{
		{Database: "db", Table: "a", Parts: 1, WrittenRows: 10, WrittenBytes: 100},
		{Database: "db", Table: "b", Parts: 2, WrittenRows: 20, WrittenBytes: 200},
		{Database: "db", Table: "c", Parts: 3, WrittenRows: 30, WrittenBytes: 300},
	}
	kept, other := reduceWriteRows(rows, 1)
	if len(kept) != 1 || other == nil {
		t.Fatalf("kept=%d other=%v", len(kept), other)
	}
	if other.Parts != 5 || other.WrittenRows != 50 || other.WrittenBytes != 500 {
		t.Fatalf("other = %+v", other)
	}
}
