package config

import (
	"encoding/json"
	"log/slog"
)

const redacted = "[redacted]"

// Secret is a credential (caller token, upstream API key). It formats,
// logs and marshals as "[redacted]" so it cannot leak through a log line or
// a config dump. Call Reveal to get the raw value.
type Secret string

// Reveal returns the raw secret value.
func (s Secret) Reveal() string { return string(s) }

// IsSet reports whether the secret is non-empty.
func (s Secret) IsSet() bool { return s != "" }

// String implements fmt.Stringer.
func (s Secret) String() string { return s.mask() }

// GoString implements fmt.GoStringer, covering %#v.
func (s Secret) GoString() string { return s.mask() }

// LogValue implements slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.mask()) }

// MarshalJSON implements json.Marshaler.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(s.mask()) }

// MarshalYAML implements yaml.Marshaler.
func (s Secret) MarshalYAML() (any, error) { return s.mask(), nil }

func (s Secret) mask() string {
	if s == "" {
		return ""
	}
	return redacted
}
