package outcome

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestOK(t *testing.T) {
	tests := []struct {
		status int
		err    string
		want   bool
	}{
		{200, "", true}, {204, "", true}, {299, "", true},
		{199, "", false}, {300, "", false}, {503, ErrHTTPStatus, false},
		{200, ErrStreamError, false}, {200, ErrTruncated, false}, {0, ErrConnect, false},
	}
	for _, tc := range tests {
		if got := OK(tc.status, tc.err); got != tc.want {
			t.Errorf("OK(%d, %q) = %v, want %v", tc.status, tc.err, got, tc.want)
		}
	}
}

func TestStatusKey(t *testing.T) {
	for _, tc := range []struct {
		status int
		err    string
		want   string
	}{{200, "", "200"}, {503, "", "503"}, {503, ErrHTTPStatus, "error:http_status"}, {0, ErrConnect, "error:connect"}} {
		if got := StatusKey(tc.status, tc.err); got != tc.want {
			t.Errorf("StatusKey(%d, %q) = %q, want %q", tc.status, tc.err, got, tc.want)
		}
	}
}

func TestPercentilesNearestRank(t *testing.T) {
	var v []float64
	for i := 100; i >= 1; i-- {
		v = append(v, float64(i))
	}
	got := Percentiles(v)
	want := Stats{Count: 100, MeanMS: 50.5, P50MS: 50, P95MS: 95, P99MS: 99, MaxMS: 100}
	if got != want {
		t.Fatalf("Percentiles = %+v, want %+v", got, want)
	}
	if (Percentiles(nil) != Stats{}) {
		t.Fatal("empty input: want zero stats")
	}
	// A 1 ms shift on second-long latencies is resolved (a 1% histogram would not).
	a, b := Percentiles([]float64{800, 800, 800}), Percentiles([]float64{801, 801, 801})
	if b.P50MS-a.P50MS != 1 {
		t.Fatalf("p50 shift = %v, want 1", b.P50MS-a.P50MS)
	}
	// Nearest rank on 10 samples: p95 is the 10th, p50 the 5th.
	ten := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if Rank(ten, 0.95) != 10 || Rank(ten, 0.50) != 5 || Rank(ten, 0.10) != 1 {
		t.Fatalf("Rank on 1..10: p95 %v p50 %v p10 %v, want 10, 5, 1", Rank(ten, 0.95), Rank(ten, 0.50), Rank(ten, 0.10))
	}
	if !math.IsNaN(Rank(nil, 0.5)) {
		t.Fatal("Rank of nothing should be NaN")
	}
}

func TestBurstWindows(t *testing.T) {
	w := BurstWindows{Every: 60 * time.Second, Length: 30 * time.Second}
	for _, tc := range []struct {
		at   time.Duration
		want bool
	}{
		{0, false}, {29 * time.Second, false}, // k = 0 is never a window
		{60 * time.Second, true}, {89*time.Second + 999*time.Millisecond, true},
		{90 * time.Second, false}, {119 * time.Second, false},
		{120 * time.Second, true}, {150 * time.Second, false},
	} {
		if got := w.Contains(tc.at); got != tc.want {
			t.Errorf("Contains(%v) = %v, want %v", tc.at, got, tc.want)
		}
	}
	if (BurstWindows{}).Contains(90 * time.Second) {
		t.Fatal("zero windows contain nothing")
	}
}

func TestMS(t *testing.T) {
	if got := MS(1500 * time.Microsecond); got != 1.5 {
		t.Fatalf("MS(1.5ms) = %v", got)
	}
	if got := MS(1234567 * time.Nanosecond); got != 1.235 {
		t.Fatalf("MS rounds to microseconds: got %v, want 1.235", got)
	}
}

func TestReadRecords(t *testing.T) {
	in := `{"seq":1,"class":"a","status":200,"ok":true,"complete":true,"ttft_ms":5,"first_byte_ms":2}
{"seq":2,"class":"a","status":0,"error":"connect"}
`
	var got []Record
	if err := ReadRecords(strings.NewReader(in), 0, func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].OK || !got[0].Succeeded() || got[0].FirstByteMS != 2 || got[1].Succeeded() {
		t.Fatalf("records = %+v", got)
	}
	if err := ReadRecords(strings.NewReader(in), 1, func(Record) error { return nil }); err == nil {
		t.Fatal("limit 1 on 2 records: want an error")
	}
	if err := ReadRecords(strings.NewReader("{\nnot json\n"), 0, func(Record) error { return nil }); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("malformed line: err = %v, want one naming line 1", err)
	}
}

// TestRecordSucceededMatchesOK: a record written before the ok field existed
// is judged by the same rule.
func TestRecordSucceededMatchesOK(t *testing.T) {
	for _, r := range []Record{{Status: 200}, {Status: 200, Error: ErrTruncated}, {Status: 503, Error: ErrHTTPStatus}} {
		if r.Succeeded() != OK(r.Status, r.Error) {
			t.Fatalf("Succeeded(%+v) disagrees with OK", r)
		}
	}
}
