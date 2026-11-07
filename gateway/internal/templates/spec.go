package templates

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxSpecBytes bounds an encoded spec: the admin API body and the store's
// spec column.
const MaxSpecBytes = 1 << 20

// ParseSpec decodes a spec strictly: unknown fields and trailing data are
// errors. It does not validate; see Validate.
func ParseSpec(data []byte) (Spec, error) {
	if len(data) > MaxSpecBytes {
		return Spec{}, fmt.Errorf("spec is %d bytes, over the %d-byte limit", len(data), MaxSpecBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var s Spec
	if err := dec.Decode(&s); err != nil {
		return Spec{}, fmt.Errorf("decode spec: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Spec{}, errors.New("decode spec: trailing data after the spec object")
	}
	return s, nil
}
