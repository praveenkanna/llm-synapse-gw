package affinity

// ExtractWithBudget is Extract's body scan with the pre-key budget set to
// budget work units, so BenchmarkExtractBudgetSweep can measure the cost of
// candidate budgets without changing the constant.
func ExtractWithBudget(body []byte, budget int) Result { return extractFromBody(body, budget*unit) }

// PreKeyBudget and PostKeyBudget expose the budgets to the tests.
const (
	PreKeyBudget  = preKeyBudget / unit
	PostKeyBudget = postKeyBudget / unit
)

// ExtractStats runs Extract's body scan and also returns the work units it
// spent and its final work limit (the pre-key budget, or less once a
// messages key lowered it to the post-key cap), so tests can assert the
// budget bounds the scan without timing it.
func ExtractStats(body []byte) (res Result, work, limit int) {
	s := scanner{b: body, limit: preKeyBudget}
	res = s.extract()
	return res, (s.work + unit - 1) / unit, s.limit / unit
}

// IsKey exposes isKey to the tests.
var IsKey = isKey

// SkipValueAll skips the JSON value at the start of b (after whitespace)
// with no work limit and reports where it ended and whether it succeeded.
func SkipValueAll(b []byte) (end int, ok bool) {
	s := scanner{b: b, limit: 1 << 40}
	end, err := s.skipValue(s.skipSpace(0))
	return end, err == scanOK
}

// QuoteOrBackslash exposes quoteOrBackslash to the tests.
var QuoteOrBackslash = quoteOrBackslash

// SkipStringAll skips the string at b[i] == '"' with no work limit and
// reports where it ended and whether it closed.
func SkipStringAll(b []byte, i int) (end int, ok bool) {
	s := scanner{b: b, limit: 1 << 40}
	end, err := s.skipString(i)
	return end, err == scanOK
}
