package bodyindextest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
)

// Seeds are bodies every reader's fuzz test starts from: the shapes the
// gateway sees, and the edges where hand-written readers tend to differ.
var Seeds = []string{
	`{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
	`{"model":"m","stream":true,"priority":5,"max_tokens":10}`,
	` { "model" : "m" , "stream" : false , "max_completion_tokens" : 3 } `,
	"{\n\t\"model\":\"m\",\r\n\"priority\":-0.0e5}",
	`{"priority":1,"model":"m","priority":2}`,
	`{"model":"a","model":"b","stream":true,"stream":"x"}`,
	`{"model":"escaped key","st\"ream":true}`,
	`{"model":"café ☃ \"quoted\"","prompt":"x\\y"}`,
	`{"messages":[{"content":"{\"nested\":[1,{\"a\":\"}\"}]}"}],"n":2}`,
	`{"a":{"b":{"c":[[],{}]}},"model":null,"stream":null}`,
	`{"n":1e300,"t":-12.5E-3,"u":0,"v":true,"w":false}`,
	`{}`,
	`{ }`,
	`[]`,
	`"model"`,
	`{"priority":1,"priority":2,"model":"m","stream":true,"max_tokens":1}`,
	`{"pri\u006frity":1,"m\u006fdel":"m","str\u0065am":true,"max_t\u006fkens":2,"max_completion_t\u006fkens":3,"priority":4}`,
	`{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"j":10,"k":11,"l":12,"m":13,"n":14,"o":15,"p":16,"q":17,"model":"m","priority":0}`,
	`{"model":"m","temperature":0,"user":"a","user":5}`,
	`{"model":"m","temperature":0,"user":5}`,
	`{"model":"m","temperature":0,"user":1,"user":2,"user":5}`,
	`{"model":"m","temperature":0,"priority":1,"priority":2,"priority":5}`,
	wide(`"model":"m","stream":true,"max_tokens":1,"priority":5,`, ``),
	wide(``, `,"model":"m","stream":true,"max_completion_tokens":2,"priority":1,"priority":5`),
	`{"` + Longest("model") + `":"m","` + Longest("stream") + `":true,"` + Longest("priority") + `":3,"` + Longest("max_tokens") + `":1}`,
	`{"` + Longest("model") + `a":"m","model":"n"}`,
	`{"` + strings.ReplaceAll(Longest("mo"), "6d", "6D") + `":1,"` + Longest("model") + `a":2}`,
	`{"` + strings.Repeat(Longest("a"), 1<<12) + `":1,"model":"m"}`,
	`{"` + strings.Repeat(Longest("a")+`":0,"`, bodyindex.MaxMembers+1) + Longest("model") + `":"m"}`,
	`{"model":"m"} {"model":"n"}`,
	`{"model":"m",}`,
	`{"model":`,
	"",
}

// Longest spells name with every byte a Unicode escape, the longest spelling
// JSON has of it.
func Longest(name string) string {
	var b strings.Builder
	for i := range len(name) {
		fmt.Fprintf(&b, "%cu%04x", 0x5c, name[i])
	}
	return b.String()
}

// wide is an object past bodyindex.MaxMembers with first and last around
// its filler: the fields a reader must still find there.
func wide(first, last string) string {
	var b strings.Builder
	b.WriteString("{" + first)
	for i := range bodyindex.MaxMembers + 1 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"x%d":0`, i)
	}
	b.WriteString(last + "}")
	return b.String()
}

// AddSeeds adds Seeds and the wire goldens' bodies to f's corpus. dir is
// the module's testdata/wire directory, relative to the calling test.
func AddSeeds(f *testing.F, dir string) {
	for _, s := range Seeds {
		f.Add([]byte(s))
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.body.json"))
	if err != nil || len(files) == 0 {
		f.Fatalf("no wire golden bodies in %s (%v): the seeds would silently shrink", dir, err)
	}
	for _, p := range files {
		if b, err := os.ReadFile(p); err == nil {
			f.Add(b)
		}
	}
}
