package templates

import (
	"errors"
	"strconv"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
)

// Fail-open reasons of the compile stage: why a request carrying
// X-Template-Id was forwarded without a template label.
const (
	ReasonRegistryUnavailable = "registry_unavailable"
	ReasonUnknownTemplate     = "unknown_template"
	ReasonNoVersion           = "no_version"
	ReasonBadVersion          = "bad_version"
	ReasonUnknownVersion      = "unknown_version"
	ReasonUnparseableMessages = "unparseable_messages"
	ReasonUnsupportedContent  = "unsupported_content"
	ReasonStaticMismatch      = "static_mismatch"
)

// Match checks a request against the version its headers name: id from
// X-Template-Id, versionHeader from X-Template-Version, body the chat
// completions request. It returns the version when the request's messages
// open with its static prefix, else nil and a fail-open reason. It never
// modifies body and allocates nothing on the request path.
func (s *Snapshot) Match(id, versionHeader string, body []byte) (*Version, string) {
	v, reason := s.lookupForMatch(id, versionHeader)
	if v == nil {
		return nil, reason
	}
	open, ok := bodyindex.ValidObject(body)
	if !ok {
		return nil, ReasonUnparseableMessages
	}
	m, found := bodyindex.FindLast(body, open, "messages")
	if !found || m.Kind != bodyindex.Array {
		return nil, ReasonUnparseableMessages
	}
	if reason := matchMessagesArray(body[m.Value[0]:m.Value[1]], &v.prefix); reason != "" {
		return nil, reason
	}
	return v, ""
}

// MatchIndex is Match over an already-indexed request body (AD-15), avoiding
// a second validation or top-level scan of body.
func (s *Snapshot) MatchIndex(id, versionHeader string, ix *bodyindex.Index) (*Version, string) {
	v, reason := s.lookupForMatch(id, versionHeader)
	if v == nil {
		return nil, reason
	}
	if ix == nil {
		return nil, ReasonUnparseableMessages
	}
	var msgsRaw []byte
	switch err := ix.Err(); {
	case err == nil:
		m, count := ix.Find("messages")
		if count == 0 || m.Kind != bodyindex.Array {
			return nil, ReasonUnparseableMessages
		}
		msgsRaw = ix.Value(m)
	case errors.Is(err, bodyindex.ErrTooManyMembers):
		var last bodyindex.Member
		ix.Walk(func(m bodyindex.Member) {
			if ix.Is(m, "messages") {
				last = m
			}
		})
		if last.Kind != bodyindex.Array {
			return nil, ReasonUnparseableMessages
		}
		msgsRaw = ix.Value(last)
	default:
		return nil, ReasonUnparseableMessages
	}
	if reason := matchMessagesArray(msgsRaw, &v.prefix); reason != "" {
		return nil, reason
	}
	return v, ""
}

func (s *Snapshot) lookupForMatch(id, versionHeader string) (*Version, string) {
	if s == nil {
		return nil, ReasonRegistryUnavailable
	}
	if len(id) > MaxIDBytes { // no snapshot holds one: never hashed
		return nil, ReasonUnknownTemplate
	}
	vs, ok := s.byID[id]
	if !ok {
		return nil, ReasonUnknownTemplate
	}
	n, reason := parseVersion(versionHeader)
	if reason != "" {
		return nil, reason
	}
	v, ok := find(vs, n)
	if !ok {
		return nil, ReasonUnknownVersion
	}
	return v, ""
}

// matchMessagesArray walks msgs, a validated JSON array slice, checking that
// every element is an object (or null) whose last "role" (if present) is a
// string, and that the leading elements match p's static prefix. Keys match
// case-sensitively and the last duplicate key wins, matching vLLM's parser.
func matchMessagesArray(msgs []byte, p *Prefix) string {
	firstFailure := ""
	elemIdx := 0
	for i := bodyindex.SkipSpace(msgs, 1); msgs[i] != ']'; {
		var hasRole, hasContent bool
		var roleRaw, contentRaw []byte
		var end int
		switch msgs[i] {
		case '{':
			j := bodyindex.SkipSpace(msgs, i+1)
			for msgs[j] != '}' {
				ks := j
				ke := bodyindex.SkipString(msgs, ks)
				key := msgs[ks:ke]
				vs := bodyindex.SkipSpace(msgs, bodyindex.SkipSpace(msgs, ke)+1)
				ve := bodyindex.SkipValue(msgs, vs)
				if bodyindex.KeyIs(key, "role") {
					hasRole, roleRaw = true, msgs[vs:ve]
				} else if bodyindex.KeyIs(key, "content") {
					hasContent, contentRaw = true, msgs[vs:ve]
				}
				j = bodyindex.SkipSpace(msgs, ve)
				if msgs[j] == ',' {
					j = bodyindex.SkipSpace(msgs, j+1)
				}
			}
			end = j + 1
			if hasRole && (len(roleRaw) == 0 || roleRaw[0] != '"') {
				return ReasonUnparseableMessages
			}
		case 'n':
			end = i + 4
		default:
			return ReasonUnparseableMessages
		}

		if firstFailure == "" {
			switch {
			case elemIdx < len(p.Messages):
				want := p.Messages[elemIdx]
				if !hasContent || len(contentRaw) == 0 || contentRaw[0] != '"' {
					firstFailure = ReasonUnsupportedContent
				} else {
					roleOK := (!hasRole && want.Role == "") || (hasRole && jsonStringMatch(roleRaw, want.Role, true))
					if !roleOK || !jsonStringMatch(contentRaw, want.Content, true) {
						firstFailure = ReasonStaticMismatch
					}
				}
			case p.Role != "" && elemIdx == len(p.Messages):
				if !hasContent || len(contentRaw) == 0 || contentRaw[0] != '"' {
					firstFailure = ReasonUnsupportedContent
				} else {
					roleOK := hasRole && jsonStringMatch(roleRaw, p.Role, true)
					if !roleOK || !jsonStringMatch(contentRaw, p.Text, false) {
						firstFailure = ReasonStaticMismatch
					}
				}
			}
		}

		elemIdx++
		i = bodyindex.SkipSpace(msgs, end)
		if msgs[i] == ',' {
			i = bodyindex.SkipSpace(msgs, i+1)
		}
	}
	if firstFailure != "" {
		return firstFailure
	}
	need := len(p.Messages)
	if p.Role != "" {
		need++
	}
	if elemIdx < need {
		return ReasonStaticMismatch
	}
	return ""
}

// maxVersionHeaderBytes is the longest X-Template-Version read: ten digits
// of a version with room for zeros and space. A longer one is a bad version.
const maxVersionHeaderBytes = 32

// parseVersion parses X-Template-Version: a positive decimal integer.
func parseVersion(h string) (int, string) {
	if len(h) > maxVersionHeaderBytes {
		return 0, ReasonBadVersion
	}
	h = strings.TrimSpace(h)
	if h == "" {
		return 0, ReasonNoVersion
	}
	for i := 0; i < len(h); i++ {
		if h[i] < '0' || h[i] > '9' {
			return 0, ReasonBadVersion
		}
	}
	n, err := strconv.ParseInt(h, 10, 32)
	if err != nil || n <= 0 {
		return 0, ReasonBadVersion
	}
	return int(n), ""
}
