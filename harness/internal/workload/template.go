package workload

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Template spec roles and segment kinds, as the gateway's registry defines
// them (docs/plan/phase-3-compiled.md, "Template version spec").
const (
	KindStatic     = "static"
	KindSemiStatic = "semi_static"
	KindVolatile   = "volatile"
)

// Spec ceilings, the gateway's registration limits.
const (
	maxSpecMessages  = 64
	maxSpecSegments  = 256
	maxSpecVariables = 64
	maxSpecText      = 256 << 10
	maxSpecBytes     = 1 << 20
)

// Value generator ceilings.
const (
	maxPickValues = 1024
	maxGenLength  = 100_000
	maxHexLength  = 256
)

// Value generator kinds.
const (
	GenFixed     = "fixed"     // value
	GenPick      = "pick"      // one of values, uniformly
	GenHex       = "hex"       // length lower-case hex digits
	GenUUID      = "uuid"      // random version-4 UUID
	GenWords     = "words"     // synthetic words, exactly length characters
	GenTimestamp = "timestamp" // RFC 3339 UTC second in 2026
	GenInt       = "int"       // decimal integer in [min, max], uniformly
)

var (
	roles = map[string]bool{"system": true, "developer": true, "user": true, "assistant": true}
	kinds = map[string]bool{KindStatic: true, KindSemiStatic: true, KindVolatile: true}

	// placeholderRE matches one {{name}} placeholder.
	placeholderRE = regexp.MustCompile(`\{\{([a-z][a-z0-9_]{0,63})\}\}`)
	variableRE    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	// templateIDRE is the gateway's template ID pattern.
	templateIDRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

	timestampBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// Template makes a class send chat messages rendered from a template
// version spec instead of synthetic words. ID and Version, when both set,
// are sent as X-Template-Id and X-Template-Version (ADR 0006); unset, the
// same spec runs as plain requests.
type Template struct {
	// Spec is the spec file (JSON, the gateway's format), relative to the
	// profile file.
	Spec      string              `yaml:"spec" json:"spec"`
	ID        string              `yaml:"id,omitempty" json:"id,omitempty"`
	Version   int                 `yaml:"version,omitempty" json:"version,omitempty"`
	Variables map[string]ValueGen `yaml:"variables" json:"variables"`
	// SpecSHA256 is the loaded spec file's digest, recorded in run.json.
	SpecSHA256 string `yaml:"-" json:"spec_sha256,omitempty"`

	spec *TemplateSpec
}

// ValueGen generates one variable's value per request. In YAML it is a
// plain string (fixed) or a mapping:
//
//	{gen: fixed, value: acme}
//	{gen: pick, values: [a, b, c]}
//	{gen: hex, length: 16}
//	{gen: uuid}
//	{gen: words, length: 180}      # synthetic words, exactly 180 characters
//	{gen: timestamp}               # e.g. 2026-03-14T09:26:53Z
//	{gen: int, min: 0, max: 999}   # one of 1,000 values: a bounded key space
type ValueGen struct {
	Gen    string   `yaml:"gen" json:"gen"`
	Value  string   `yaml:"value,omitempty" json:"value,omitempty"`
	Values []string `yaml:"values,omitempty" json:"values,omitempty"`
	Length int      `yaml:"length,omitempty" json:"length,omitempty"`
	Min    int64    `yaml:"min,omitempty" json:"min,omitempty"`
	Max    int64    `yaml:"max,omitempty" json:"max,omitempty"`
}

// UnmarshalYAML accepts a plain string as shorthand for a fixed value.
func (v *ValueGen) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		*v = ValueGen{Gen: GenFixed, Value: n.Value}
		return nil
	}
	type plain ValueGen // no UnmarshalYAML: avoids recursion
	var p plain
	if err := n.Decode(&p); err != nil {
		return err
	}
	*v = ValueGen(p)
	return nil
}

func (v ValueGen) validate() error {
	if v.Gen != GenInt && (v.Min != 0 || v.Max != 0) {
		return fmt.Errorf("%s takes no min or max", v.Gen)
	}
	switch v.Gen {
	case GenInt:
		if v.Min < 0 || v.Max < v.Min || v.Max == math.MaxInt64 || v.Value != "" || len(v.Values) > 0 || v.Length != 0 {
			return errors.New("int takes only min and max, 0 <= min <= max")
		}
	case GenFixed:
		if len(v.Values) > 0 || v.Length != 0 {
			return errors.New("fixed takes only value")
		}
	case GenPick:
		if len(v.Values) == 0 || len(v.Values) > maxPickValues {
			return fmt.Errorf("pick needs 1 to %d values", maxPickValues)
		}
		if v.Value != "" || v.Length != 0 {
			return errors.New("pick takes only values")
		}
	case GenHex:
		if v.Length < 1 || v.Length > maxHexLength || v.Value != "" || len(v.Values) > 0 {
			return fmt.Errorf("hex takes only length in [1, %d]", maxHexLength)
		}
	case GenWords:
		if v.Length < 1 || v.Length > maxGenLength || v.Value != "" || len(v.Values) > 0 {
			return fmt.Errorf("words takes only length in [1, %d]", maxGenLength)
		}
	case GenUUID, GenTimestamp:
		if v.Value != "" || len(v.Values) > 0 || v.Length != 0 {
			return fmt.Errorf("%s takes no parameters", v.Gen)
		}
	case "":
		return errors.New("missing gen (fixed, pick, hex, uuid, words, timestamp or int)")
	default:
		return fmt.Errorf("unknown gen %q (want fixed, pick, hex, uuid, words, timestamp or int)", v.Gen)
	}
	return nil
}

// sample draws one value. v must be valid.
func (v ValueGen) sample(rng *rand.Rand) string {
	switch v.Gen {
	case GenPick:
		return v.Values[rng.IntN(len(v.Values))]
	case GenHex:
		return randHex(rng, v.Length)
	case GenUUID:
		h := []byte(randHex(rng, 32))
		h[12] = '4'
		h[16] = "89ab"[rng.IntN(4)]
		return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
	case GenWords:
		return wordChars(rng, v.Length)
	case GenTimestamp:
		return timestampBase.Add(time.Duration(rng.Int64N(365*24*3600)) * time.Second).Format(time.RFC3339)
	case GenInt:
		return strconv.FormatInt(v.Min+rng.Int64N(v.Max-v.Min+1), 10)
	default:
		return v.Value
	}
}

func randHex(rng *rand.Rand, n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = digits[rng.IntN(len(digits))]
	}
	return string(b)
}

// wordChars returns synthetic words of exactly n characters, never ending
// in a space.
func wordChars(rng *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n + 8)
	for b.Len() < n {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(syllables[rng.IntN(len(syllables))])
		b.WriteString(syllables[rng.IntN(len(syllables))])
	}
	s := []byte(b.String()[:n])
	if s[n-1] == ' ' {
		s[n-1] = 'a'
	}
	return string(s)
}

// TemplateSpec is a template version spec in the gateway's JSON format.
// The harness renders it; it does not check segment order, so a misordered
// spec (rejected by the gateway's registry) still renders as today's
// callers send it.
type TemplateSpec struct {
	Model       string              `json:"model"`
	Description string              `json:"description,omitempty"`
	Variables   []SpecVariable      `json:"variables,omitempty"`
	Messages    []SpecMessage       `json:"messages"`
	Examples    []map[string]string `json:"examples,omitempty"`
	Cacheable   bool                `json:"cacheable,omitempty"`
	CacheTTL    string              `json:"cache_ttl,omitempty"`
	Coalesce    bool                `json:"coalesce,omitempty"`
}

// SpecVariable declares a placeholder name and, optionally, its scope.
type SpecVariable struct {
	Name  string `json:"name"`
	Scope string `json:"scope,omitempty"` // "shared", "user" or unset
}

// Variable scopes. A spec that sets coalesce or cacheable must mark every
// variable shared, as the gateway's registry requires.
const (
	ScopeShared = "shared"
	ScopeUser   = "user"
)

// SpecMessage is one templated chat message; its content is its segments'
// texts joined with nothing in between.
type SpecMessage struct {
	Role     string        `json:"role"`
	Segments []SpecSegment `json:"segments"`
}

// SpecSegment is a run of text of one kind. Static text is literal;
// semi_static and volatile text may hold {{name}} placeholders.
type SpecSegment struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// LoadSpec reads and checks a template spec file. It returns the spec and
// the file's SHA-256.
func LoadSpec(path string) (*TemplateSpec, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("template spec: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxSpecBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("template spec %s: %w", path, err)
	}
	s, err := ParseSpec(b)
	if err != nil {
		return nil, "", fmt.Errorf("template spec %s: %w", path, err)
	}
	sum := sha256.Sum256(b)
	return s, hex.EncodeToString(sum[:]), nil
}

// ParseSpec decodes and checks a template spec. Unknown fields are rejected.
func ParseSpec(b []byte) (*TemplateSpec, error) {
	if len(b) > maxSpecBytes {
		return nil, fmt.Errorf("larger than %d bytes", maxSpecBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var s TemplateSpec
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data after the spec")
	}
	return &s, s.validate()
}

// validate applies the gateway's registration checks except segment order:
// structure, roles and kinds, variables and examples, and limits.
func (s *TemplateSpec) validate() error {
	if s.Model == "" {
		return errors.New("model is required")
	}
	if len(s.Messages) == 0 || len(s.Messages) > maxSpecMessages {
		return fmt.Errorf("want 1 to %d messages, got %d", maxSpecMessages, len(s.Messages))
	}
	if len(s.Variables) > maxSpecVariables {
		return fmt.Errorf("more than %d variables", maxSpecVariables)
	}
	declared := map[string]bool{}
	for i, v := range s.Variables {
		if !variableRE.MatchString(v.Name) {
			return fmt.Errorf("variables[%d]: name %q must match [a-z][a-z0-9_]{0,63}", i, v.Name)
		}
		if declared[v.Name] {
			return fmt.Errorf("variables[%d]: %q declared twice", i, v.Name)
		}
		declared[v.Name] = true
		switch {
		case v.Scope != "" && v.Scope != ScopeShared && v.Scope != ScopeUser:
			return fmt.Errorf("variables[%d].scope: unknown scope %q", i, v.Scope)
		case (s.Coalesce || s.Cacheable) && v.Scope != ScopeShared:
			return fmt.Errorf("variables[%d].scope: must be %q when the spec sets coalesce or cacheable", i, ScopeShared)
		}
	}
	used := map[string]bool{}
	segments, text := 0, 0
	for i, m := range s.Messages {
		if !roles[m.Role] {
			return fmt.Errorf("messages[%d]: unknown role %q", i, m.Role)
		}
		if len(m.Segments) == 0 {
			return fmt.Errorf("messages[%d]: no segments", i)
		}
		for j, seg := range m.Segments {
			path := fmt.Sprintf("messages[%d].segments[%d]", i, j)
			segments++
			text += len(seg.Text)
			names := placeholderRE.FindAllStringSubmatch(seg.Text, -1)
			switch {
			case !kinds[seg.Kind]:
				return fmt.Errorf("%s: unknown kind %q", path, seg.Kind)
			case seg.Kind == KindStatic && len(names) > 0:
				return fmt.Errorf("%s: static text holds placeholder %s; mark it semi_static or volatile", path, names[0][0])
			}
			for _, n := range names {
				if !declared[n[1]] {
					return fmt.Errorf("%s: placeholder %s names an undeclared variable", path, n[0])
				}
				used[n[1]] = true
			}
		}
	}
	if segments > maxSpecSegments || text > maxSpecText {
		return fmt.Errorf("more than %d segments or %d bytes of text", maxSpecSegments, maxSpecText)
	}
	for _, v := range s.Variables {
		if !used[v.Name] {
			return fmt.Errorf("variable %q is declared but never used", v.Name)
		}
	}
	for i, ex := range s.Examples {
		for _, v := range s.Variables {
			if _, ok := ex[v.Name]; !ok {
				return fmt.Errorf("examples[%d]: missing variable %q", i, v.Name)
			}
		}
		for name := range ex {
			if !declared[name] {
				return fmt.Errorf("examples[%d]: unknown variable %q", i, name)
			}
		}
	}
	return nil
}

// render returns the spec's chat messages with every {{name}} placeholder in
// semi_static and volatile text replaced by vars[name], verbatim. Static
// text is copied literally. A placeholder with no value is an error;
// substituted values are not expanded again.
func (s *TemplateSpec) render(vars map[string]string) ([]chatMessage, error) {
	out := make([]chatMessage, len(s.Messages))
	for i, m := range s.Messages {
		var b strings.Builder
		for j, seg := range m.Segments {
			if seg.Kind == KindStatic {
				b.WriteString(seg.Text)
				continue
			}
			rest := seg.Text
			for {
				loc := placeholderRE.FindStringSubmatchIndex(rest)
				if loc == nil {
					b.WriteString(rest)
					break
				}
				name := rest[loc[2]:loc[3]]
				v, ok := vars[name]
				if !ok {
					return nil, fmt.Errorf("messages[%d].segments[%d]: no value for variable %q", i, j, name)
				}
				b.WriteString(rest[:loc[0]])
				b.WriteString(v)
				rest = rest[loc[1]:]
			}
		}
		out[i] = chatMessage{Role: m.Role, Content: b.String()}
	}
	return out, nil
}

// validate checks the template block of a class. The spec must be loaded.
func (t *Template) validate() error {
	switch {
	case t.Spec == "":
		return errors.New("spec is required")
	case t.spec == nil:
		return fmt.Errorf("spec %s not loaded", t.Spec)
	case (t.ID == "") != (t.Version == 0):
		return errors.New("id and version go together: set both to send the template headers, or neither")
	case t.ID != "" && !templateIDRE.MatchString(t.ID):
		return fmt.Errorf("id %q must match %s", t.ID, templateIDRE)
	case t.Version < 0:
		return errors.New("version must be a positive integer")
	}
	for _, v := range t.spec.Variables {
		g, ok := t.Variables[v.Name]
		if !ok {
			return fmt.Errorf("variables: no generator for %q", v.Name)
		}
		if err := g.validate(); err != nil {
			return fmt.Errorf("variables.%s: %w", v.Name, err)
		}
	}
	for name := range t.Variables {
		if !t.declares(name) {
			return fmt.Errorf("variables: %q is not declared by the spec", name)
		}
	}
	return nil
}

func (t *Template) declares(name string) bool {
	for _, v := range t.spec.Variables {
		if v.Name == name {
			return true
		}
	}
	return false
}

// load reads the spec file, resolving a relative path against dir.
func (t *Template) load(dir string) error {
	path := t.Spec
	if path == "" {
		return nil // validate reports it
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, path)
	}
	s, sum, err := LoadSpec(path)
	if err != nil {
		return err
	}
	t.spec, t.SpecSHA256 = s, sum
	return nil
}

// sampleVars draws one value per declared variable, in declaration order so
// a seed fixes the sequence.
func (t *Template) sampleVars(rng *rand.Rand) map[string]string {
	vars := make(map[string]string, len(t.spec.Variables))
	for _, v := range t.spec.Variables {
		vars[v.Name] = t.Variables[v.Name].sample(rng)
	}
	return vars
}
