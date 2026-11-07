package templates

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Version is one served template version, compiled for matching. It is
// shared by every request: never modify it.
type Version struct {
	ID      string
	Version int
	Status  Status
	Spec    Spec
	Counts  Counts

	prefix   Prefix
	cacheTTL time.Duration
}

// Coalescible reports whether identical requests of this version may share
// one upstream generation (the template set coalesce: true; registration
// already required every variable to be shared).
func (v *Version) Coalescible() bool { return v.Spec.Coalesce }

// CacheTTL is how long a response of this version may be cached; zero when
// the template is not cacheable.
func (v *Version) CacheTTL() time.Duration { return v.cacheTTL }

// Prefix is the version's static prefix.
func (v *Version) Prefix() Prefix { return v.prefix }

// PrefixTokens is the registered token count every render shares; 0 when
// unknown.
func (v *Version) PrefixTokens() int { return v.Counts.PrefixTokens }

// RecordError is why NewSnapshot skipped a record. It never holds template
// text. ID is the record's ID as stored, which may fail ValidID.
type RecordError struct {
	ID      string
	Version int
	Err     error
}

func (e *RecordError) Error() string {
	return fmt.Sprintf("template %.128q version %d: %v", e.ID, e.Version, e.Err)
}

func (e *RecordError) Unwrap() error { return e.Err }

// Snapshot is an immutable set of served versions, read by the request
// path without locks.
type Snapshot struct {
	byID     map[string][]*Version // each sorted by version
	versions []*Version            // sorted by ID, version
	loadedAt time.Time
}

// NewSnapshot compiles the served records. It skips, with one error each,
// a record that isn't served, has an invalid ID or version, a spec that
// doesn't parse or validate, unparseable counts, or duplicates an earlier
// record.
func NewSnapshot(recs []Record, loadedAt time.Time) (*Snapshot, []error) {
	s := &Snapshot{byID: make(map[string][]*Version), loadedAt: loadedAt}
	var errs []error
	seen := make(map[versionKey]bool, len(recs))
	for _, r := range recs {
		v, err := compile(r)
		if err == nil && seen[versionKey{r.ID, r.Version}] {
			err = errors.New("duplicate record")
		}
		if err != nil {
			errs = append(errs, &RecordError{ID: r.ID, Version: r.Version, Err: err})
			continue
		}
		seen[versionKey{r.ID, r.Version}] = true
		s.versions = append(s.versions, v)
	}
	slices.SortFunc(s.versions, func(a, b *Version) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Version, b.Version))
	})
	for _, v := range s.versions {
		s.byID[v.ID] = append(s.byID[v.ID], v)
	}
	return s, errs
}

type versionKey struct {
	id      string
	version int
}

// compile turns a record into a Version. Its errors name no template text.
func compile(r Record) (*Version, error) {
	switch {
	case !ValidID(r.ID):
		return nil, errors.New("invalid template ID")
	case r.Version <= 0:
		return nil, errors.New("version is not positive")
	case !r.Status.Served():
		return nil, fmt.Errorf("status %.32q is not served", r.Status)
	}
	spec, err := ParseSpec(r.Spec)
	if err != nil {
		return nil, errors.New("spec does not parse")
	}
	if verrs := Validate(spec); verrs != nil {
		return nil, fmt.Errorf("spec is invalid: %d problems, the first at %s", len(verrs), verrs[0].Path)
	}
	var counts Counts
	if len(r.Counts) > 0 {
		if err := json.Unmarshal(r.Counts, &counts); err != nil {
			return nil, errors.New("counts do not parse")
		}
	}
	v := &Version{ID: r.ID, Version: r.Version, Status: r.Status, Spec: spec, Counts: counts, prefix: StaticPrefix(spec)}
	if spec.Cacheable {
		v.cacheTTL, _ = time.ParseDuration(spec.CacheTTL) // validated above: positive
	}
	return v, nil
}

// Lookup returns a served version.
func (s *Snapshot) Lookup(id string, version int) (*Version, bool) {
	if s == nil {
		return nil, false
	}
	return find(s.byID[id], version)
}

func find(vs []*Version, version int) (*Version, bool) {
	i, ok := slices.BinarySearchFunc(vs, version, func(v *Version, n int) int { return cmp.Compare(v.Version, n) })
	if !ok {
		return nil, false
	}
	return vs[i], true
}

// Len is the number of served versions.
func (s *Snapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.versions)
}

// LoadedAt is when the snapshot was built.
func (s *Snapshot) LoadedAt() time.Time {
	if s == nil {
		return time.Time{}
	}
	return s.loadedAt
}

// Versions returns every served version, sorted by ID and version.
func (s *Snapshot) Versions() []*Version {
	if s == nil {
		return nil
	}
	return slices.Clone(s.versions)
}
