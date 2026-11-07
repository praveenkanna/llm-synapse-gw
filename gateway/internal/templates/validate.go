package templates

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Spec limits, enforced by Validate.
const (
	MaxMessages          = 64
	MaxSegments          = 256       // across all messages
	MaxVariables         = 64        //
	MaxTextBytes         = 256 << 10 // all segment text
	MaxExamples          = 16        //
	MaxExampleValueBytes = 64 << 10  // each variable value of an example
)

// maxReported bounds the problems Validate returns, so a pathological spec
// can't make an unbounded error list.
const maxReported = 200

// ValidationError is one problem with a spec. Path names the offending
// element, e.g. messages[0].segments[1]. Msg quotes at most short names
// (variables, roles, kinds), never segment text.
type ValidationError struct {
	Path string `json:"path"`
	Msg  string `json:"message"`
}

func (e ValidationError) Error() string {
	if e.Path == "" {
		return e.Msg
	}
	return e.Path + ": " + e.Msg
}

// ValidationErrors is every problem Validate found, in spec order.
type ValidationErrors []ValidationError

func (es ValidationErrors) Error() string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}

var validRoles = map[string]bool{"system": true, "developer": true, "user": true, "assistant": true}

// kindRank orders kinds; -1 for an unknown kind.
func kindRank(k Kind) int {
	switch k {
	case KindStatic:
		return 0
	case KindSemiStatic:
		return 1
	case KindVolatile:
		return 2
	}
	return -1
}

func segPath(i, j int) string { return fmt.Sprintf("messages[%d].segments[%d]", i, j) }

// Validate returns every problem with s (up to maxReported), or nil when s
// is valid.
func Validate(s Spec) ValidationErrors {
	var v validator
	v.structure(s)
	declared := v.variables(s.Variables)
	used := v.segments(s.Messages, declared)
	for i, vr := range s.Variables {
		if declared[vr.Name] == i && ValidName(vr.Name) && !used[vr.Name] {
			v.add(fmt.Sprintf("variables[%d]", i), "variable %q is never used in a semi_static or volatile segment", vr.Name)
		}
	}
	v.examples(s.Examples, declared)
	ttl, ttlOK := time.Duration(0), true
	if s.CacheTTL != "" {
		d, err := time.ParseDuration(s.CacheTTL)
		switch {
		case err != nil:
			v.add("cache_ttl", "%.64q is not a duration such as \"30s\"", s.CacheTTL)
			ttlOK = false
		case d < 0:
			v.add("cache_ttl", "must not be negative")
			ttlOK = false
		default:
			ttl = d
		}
	}
	if s.Cacheable && ttlOK && ttl == 0 {
		v.add("cache_ttl", "must be above 0s when cacheable is true: a cached answer needs an expiry")
	}
	v.optIn(s)
	return v.result()
}

// optIn checks that a template which shares generations between callers
// (coalesce or cacheable) declares every variable shared: the default is
// no (ADR 0019).
func (v *validator) optIn(s Spec) {
	for i, vr := range s.Variables {
		path := fmt.Sprintf("variables[%d].scope", i)
		switch vr.Scope {
		case "", ScopeShared, ScopeUser:
		default:
			v.add(path, "unknown scope %.32q; want %q or %q", vr.Scope, ScopeShared, ScopeUser)
			continue
		}
		if !s.Coalesce && !s.Cacheable || vr.Scope == ScopeShared {
			continue
		}
		what := "coalesce"
		if s.Cacheable {
			what = "cacheable"
		}
		if vr.Scope == ScopeUser {
			v.add(path, "variable %.64q is user-specific, so the template cannot set %s: its responses are shared between callers", vr.Name, what)
		} else {
			v.add(path, "variable %.64q must declare \"scope\":\"shared\" for the template to set %s: its responses are shared between callers", vr.Name, what)
		}
	}
}

type validator struct {
	errs    ValidationErrors
	dropped int
}

func (v *validator) add(path, format string, args ...any) {
	if len(v.errs) >= maxReported {
		v.dropped++
		return
	}
	v.errs = append(v.errs, ValidationError{Path: path, Msg: fmt.Sprintf(format, args...)})
}

func (v *validator) result() ValidationErrors {
	if v.dropped > 0 {
		v.errs = append(v.errs, ValidationError{Msg: fmt.Sprintf("%d more problems not shown", v.dropped)})
	}
	return v.errs
}

// structure checks the model and the size limits.
func (v *validator) structure(s Spec) {
	if strings.TrimSpace(s.Model) == "" {
		v.add("model", "required")
	}
	if len(s.Messages) == 0 {
		v.add("messages", "at least one message is required")
	}
	if n := len(s.Messages); n > MaxMessages {
		v.add("messages", "%d messages, over the limit of %d", n, MaxMessages)
	}
	segs, text := 0, 0
	for _, m := range s.Messages {
		segs += len(m.Segments)
		for _, seg := range m.Segments {
			text += len(seg.Text)
		}
	}
	if segs > MaxSegments {
		v.add("messages", "%d segments, over the limit of %d", segs, MaxSegments)
	}
	if text > MaxTextBytes {
		v.add("messages", "%d bytes of segment text, over the limit of %d", text, MaxTextBytes)
	}
	if n := len(s.Variables); n > MaxVariables {
		v.add("variables", "%d variables, over the limit of %d", n, MaxVariables)
	}
	if n := len(s.Examples); n > MaxExamples {
		v.add("examples", "%d examples, over the limit of %d", n, MaxExamples)
	}
}

// variables checks the declarations and returns each declared name's first
// index.
func (v *validator) variables(vars []Variable) map[string]int {
	declared := make(map[string]int, len(vars))
	for i, vr := range vars {
		path := fmt.Sprintf("variables[%d]", i)
		if !ValidName(vr.Name) {
			v.add(path, "name %.64q must match [a-z][a-z0-9_]{0,63}", vr.Name)
		}
		if first, dup := declared[vr.Name]; dup {
			v.add(path, "variable %.64q is already declared at variables[%d]", vr.Name, first)
			continue
		}
		declared[vr.Name] = i
	}
	return declared
}

// segments checks roles, kinds, kind order and placeholders, and returns
// the names placeholders use.
func (v *validator) segments(msgs []Message, declared map[string]int) map[string]bool {
	used := make(map[string]bool)
	firstOfRank := [3]string{} // path of the first segment of each kind
	maxRank := -1
	for i, m := range msgs {
		if !validRoles[m.Role] {
			v.add(fmt.Sprintf("messages[%d]", i), "unknown role %.32q; want system, developer, user or assistant", m.Role)
		}
		if len(m.Segments) == 0 {
			v.add(fmt.Sprintf("messages[%d]", i), "at least one segment is required")
		}
		for j, seg := range m.Segments {
			path := segPath(i, j)
			rank := kindRank(seg.Kind)
			if rank < 0 {
				v.add(path, "unknown kind %.32q; want static, semi_static or volatile", seg.Kind)
				continue
			}
			if rank < maxRank {
				v.add(path, "%s", orderMsg(seg.Kind, rankKind[maxRank], firstOfRank[maxRank]))
			}
			if rank > maxRank {
				maxRank = rank
			}
			if firstOfRank[rank] == "" {
				firstOfRank[rank] = path
			}
			v.placeholders(path, seg, declared, used)
		}
	}
	return used
}

var rankKind = [3]Kind{KindStatic, KindSemiStatic, KindVolatile}

func orderMsg(k, after Kind, afterPath string) string {
	if after == KindVolatile {
		return fmt.Sprintf("%s segment after volatile segment %s; only text before the first volatile segment can be cached, "+
			"so move volatile text after all static and semi_static text", k, afterPath)
	}
	return fmt.Sprintf("static segment after semi_static segment %s; only static text before the first semi_static segment "+
		"is shared by every caller, so move static text before all semi_static text", afterPath)
}

// placeholders checks one segment's placeholders. Names in a static segment
// count as used, so the only error for them is the one to fix.
func (v *validator) placeholders(path string, seg Segment, declared map[string]int, used map[string]bool) {
	var seen map[string]bool
	placeholders(seg.Text, func(name string) {
		if seen[name] {
			return
		}
		if seen == nil {
			seen = make(map[string]bool)
		}
		seen[name] = true
		used[name] = true
		switch _, ok := declared[name]; {
		case seg.Kind == KindStatic:
			v.add(path, "static segment contains placeholder {{%s}}; static text is literal, so mark the segment semi_static or volatile", name)
		case !ok:
			v.add(path, "placeholder {{%s}} is not declared in variables", name)
		}
	})
}

// examples checks that each example gives exactly the declared variables.
func (v *validator) examples(examples []map[string]string, declared map[string]int) {
	names := make([]string, 0, len(declared))
	for name := range declared {
		if ValidName(name) { // a bad name is already reported
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for i, ex := range examples {
		path := fmt.Sprintf("examples[%d]", i)
		for _, name := range names {
			val, ok := ex[name]
			if !ok {
				v.add(path, "missing variable %.64q", name)
			} else if len(val) > MaxExampleValueBytes {
				v.add(path, "value of %.64q is %d bytes, over the limit of %d", name, len(val), MaxExampleValueBytes)
			}
		}
		var unknown []string
		for name := range ex {
			if _, ok := declared[name]; !ok {
				unknown = append(unknown, name)
			}
		}
		slices.Sort(unknown)
		for _, name := range unknown {
			v.add(path, "unknown variable %.64q", name)
		}
	}
}
