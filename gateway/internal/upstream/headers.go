package upstream

import (
	"net/http"
	"net/textproto"
	"strings"
)

// hopByHop are the headers that apply to one connection only (RFC 9110
// section 7.6.1) and must not be forwarded by a proxy, in either direction.
var hopByHop = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// forwardHeaders returns a copy of h without hop-by-hop, Connection-listed
// or caller-only headers. Everything else, including X-Mock-* test headers,
// passes through unchanged. Value slices share read-only backing arrays with
// clipped capacity so neither map can mutate the other via Append/Add.
func forwardHeaders(h http.Header) http.Header {
	if len(h) == 0 {
		return make(http.Header, 2)
	}
	out := make(http.Header, len(h)+2)
	for k, vv := range h {
		if isDroppedRequestHeader(k) {
			continue
		}
		out[k] = vv[:len(vv):len(vv)]
	}
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			if name = textproto.TrimString(name); name != "" {
				out.Del(name)
			}
		}
	}
	return out
}

func isDroppedRequestHeader(k string) bool {
	switch k {
	case "Authorization", "Content-Length", "Host",
		"Connection", "Proxy-Connection", "Keep-Alive",
		"Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	}
	return false
}

// CopyResponseHeaders copies upstream response headers to dst, dropping
// hop-by-hop and Connection-listed ones.
func CopyResponseHeaders(dst, src http.Header) {
	for k, vv := range src {
		dst[k] = append(dst[k][:0:0], vv...)
	}
	removeHopByHop(dst)
}

func removeHopByHop(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			if name = textproto.TrimString(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, k := range hopByHop {
		h.Del(k)
	}
}
