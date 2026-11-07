// Package templates holds the prompt template registry's model: version
// specs, their validation and rendering, the static-prefix matcher the
// request path uses (ADR 0006: callers send rendered messages plus
// X-Template-Id / X-Template-Version, and the gateway only verifies), and
// the in-memory snapshot refreshed out of band from the store.
//
// The request path reads only the snapshot; it never waits on the store.
// Template text is prompt text: it is never logged.
package templates

import (
	"context"
	"encoding/json"
	"regexp"
	"time"
)

// Request headers of a Level 2 call (ADR 0006).
const (
	HeaderTemplateID      = "X-Template-Id"
	HeaderTemplateVersion = "X-Template-Version"
)

// Status is a version's lifecycle state (staged rollout, ADR 0006).
type Status string

const (
	StatusDraft      Status = "draft"      // registered, not served
	StatusActive     Status = "active"     // served
	StatusDeprecated Status = "deprecated" // served, counted separately
	StatusRetired    Status = "retired"    // no longer served; terminal
)

// Served reports whether versions in status s are in the request-path
// snapshot.
func (s Status) Served() bool { return s == StatusActive || s == StatusDeprecated }

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusDraft, StatusActive, StatusDeprecated, StatusRetired:
		return true
	}
	return false
}

// CanTransition reports whether a version may move from one status to
// another: draft → active ⇄ deprecated, and any status but retired →
// retired. Retired is terminal; moving to the same status is not a
// transition.
func CanTransition(from, to Status) bool {
	switch {
	case !from.Valid() || !to.Valid() || from == to || from == StatusRetired:
		return false
	case to == StatusRetired:
		return true
	case from == StatusDraft:
		return to == StatusActive
	case from == StatusActive:
		return to == StatusDeprecated
	case from == StatusDeprecated:
		return to == StatusActive
	}
	return false
}

// MaxIDBytes is the longest template ID idPattern accepts. No longer ID is
// in a snapshot, so X-Template-Id past it is unknown without a lookup.
const MaxIDBytes = 128

// idPattern constrains template IDs: they become metric label values.
var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// ValidID reports whether id is a well-formed template ID.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// Kind is a segment's cacheability class. Segments must appear in kind
// order: every static before any semi_static, and both before any volatile.
type Kind string

const (
	KindStatic     Kind = "static"
	KindSemiStatic Kind = "semi_static"
	KindVolatile   Kind = "volatile"
)

// Spec is a template version as registered: the admin API body and the
// store's spec column.
type Spec struct {
	Model       string              `json:"model"`
	Description string              `json:"description,omitempty"`
	Variables   []Variable          `json:"variables,omitempty"`
	Messages    []Message           `json:"messages"`
	Examples    []map[string]string `json:"examples,omitempty"`
	Cacheable   bool                `json:"cacheable,omitempty"`
	CacheTTL    string              `json:"cache_ttl,omitempty"`
	Coalesce    bool                `json:"coalesce,omitempty"`
}

// Variable scopes. A template that sets coalesce or cacheable shares one
// generation between callers, so every one of its variables must say it is
// safe to share (ADR 0019).
const (
	ScopeShared = "shared" // the value is the same for every caller who may send it
	ScopeUser   = "user"   // the value is specific to a user; the template may not opt in
)

// Variable declares a placeholder name used by the version's segments.
type Variable struct {
	Name string `json:"name"`
	// Scope is "shared", "user" or empty (undeclared). Only a template
	// whose variables are all "shared" may set coalesce or cacheable.
	Scope string `json:"scope,omitempty"`
}

// Message is one chat message of the template: its content is its
// segments' texts joined with nothing in between.
type Message struct {
	Role     string    `json:"role"`
	Segments []Segment `json:"segments"`
}

// Segment is a run of message text of one kind. Static text is literal;
// semi_static and volatile text may hold {{name}} placeholders.
type Segment struct {
	Kind Kind   `json:"kind"`
	Text string `json:"text"`
}

// Counts are a version's token counts, computed at registration through
// a replica's /tokenize (ADRs 0008, 0011). Zero values mean unknown.
type Counts struct {
	Pool         string `json:"pool,omitempty"`
	Examples     []int  `json:"examples,omitempty"`      // prompt tokens of each example's render
	PrefixTokens int    `json:"prefix_tokens,omitempty"` // tokens every render shares
	Error        string `json:"error,omitempty"`         // why counting failed, if it did
}

// Record is one stored version as the store returns it.
type Record struct {
	ID        string
	Version   int
	Status    Status
	Spec      json.RawMessage
	Counts    json.RawMessage // may be empty
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Loader is what the snapshot refresher reads from the store.
type Loader interface {
	// Stamp returns a cheap marker that changes whenever the set of served
	// versions or any of their records may have changed.
	Stamp(ctx context.Context) (string, error)
	// LoadServed returns every active and deprecated version, at most
	// limit+1 of them (so the caller can tell the set is over limit).
	LoadServed(ctx context.Context, limit int) ([]Record, error)
}
