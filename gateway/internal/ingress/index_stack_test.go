package ingress

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// indexUse is where the request path's sources declare and read a
// bodyindex.Index. Found by type, not by name, so renaming a variable
// changes nothing here.
type indexUse struct {
	values []string            // a place that makes an Index: a var, a literal, new, a value field
	params map[string]struct{} // "file:line:col name" of each *bodyindex.Index parameter
	vars   map[string]struct{} // ... of each Index variable
	reads  []string            // calls of an index's Read
}

// isIndex reports whether e names bodyindex.Index.
func isIndex(e ast.Expr) bool {
	s, ok := e.(*ast.SelectorExpr)
	if !ok || s.Sel.Name != "Index" {
		return false
	}
	pkg, ok := s.X.(*ast.Ident)
	return ok && pkg.Name == "bodyindex"
}

func isIndexPointer(e ast.Expr) bool {
	p, ok := e.(*ast.StarExpr)
	return ok && isIndex(p.X)
}

// findIndexUse reads the non-test sources of dirs.
func findIndexUse(t *testing.T, dirs ...string) indexUse {
	t.Helper()
	use := indexUse{params: map[string]struct{}{}, vars: map[string]struct{}{}}
	fset := token.NewFileSet()
	at := func(p token.Pos) string {
		pos := fset.Position(p)
		return fmt.Sprintf("%s:%d:%d", filepath.Clean(pos.Filename), pos.Line, pos.Column)
	}
	for _, dir := range dirs {
		paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(paths) == 0 {
			t.Fatalf("no sources in %s: %v", dir, err)
		}
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				findInDecl(decl, &use, at)
			}
		}
	}
	return use
}

func findInDecl(decl ast.Decl, use *indexUse, at func(token.Pos) string) {
	names := map[string]struct{}{} // the indexes in reach of this declaration, by name
	ast.Inspect(decl, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Field:
			switch {
			case isIndexPointer(n.Type):
				for _, name := range n.Names {
					names[name.Name] = struct{}{}
					use.params[at(name.Pos())+" "+name.Name] = struct{}{}
				}
			case isIndex(n.Type):
				use.values = append(use.values, at(n.Pos())+" a value field")
			}
		case *ast.ValueSpec:
			if n.Type != nil && isIndex(n.Type) {
				for _, name := range n.Names {
					names[name.Name] = struct{}{}
					use.vars[at(name.Pos())+" "+name.Name] = struct{}{}
					use.values = append(use.values, at(name.Pos())+" var "+name.Name)
				}
			}
		case *ast.CompositeLit:
			if n.Type != nil && isIndex(n.Type) {
				use.values = append(use.values, at(n.Pos())+" a literal")
			}
		case *ast.CallExpr:
			if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "new" && len(n.Args) == 1 && isIndex(n.Args[0]) {
				use.values = append(use.values, at(n.Pos())+" new")
			}
		}
		return true
	})
	ast.Inspect(decl, func(n ast.Node) bool {
		if star, ok := n.(*ast.StarExpr); ok { // a copy of the index
			if id, ok := star.X.(*ast.Ident); ok {
				if _, ok := names[id.Name]; ok {
					use.values = append(use.values, at(star.Pos())+" a copy of "+id.Name)
				}
			}
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Read" {
			return true
		}
		switch x := sel.X.(type) {
		case *ast.Ident:
			if _, ok := names[x.Name]; ok {
				use.reads = append(use.reads, at(call.Pos()))
			}
		case *ast.SelectorExpr: // a field holding one, as dedup.Request's does
			if x.Sel.Name == "Index" {
				use.reads = append(use.reads, at(call.Pos()))
			}
		}
		return true
	})
}

// sourcePackages returns every directory under internal/ with Go sources,
// this one as ".", but for bodyindex itself.
func sourcePackages(t *testing.T) []string {
	t.Helper()
	dirs := []string{"."}
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case !d.IsDir():
			return nil
		case d.Name() == "testdata", path == filepath.Join("..", "bodyindex"):
			return filepath.SkipDir
		case path == "..", path == filepath.Join("..", "ingress"):
			return nil
		}
		if sources, _ := filepath.Glob(filepath.Join(path, "*.go")); len(sources) > 0 {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

// TestOneIndexStaysOnTheStack (E3): a request has one bodyindex.Index,
// declared in handleGenerate, read once and passed down by pointer, and no
// stage keeps it, so it stays on the handler's stack. The sources of every
// package say how many there are and where, by type; the compiler's escape
// analysis of the packages it goes through says whether each stayed. It has no build tag, so it holds under -race and on
// any toolchain, unlike an allocation count: nothing here is a number to
// keep up to date.
func TestOneIndexStaysOnTheStack(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command, so no sources or compiler to ask: ", err)
	}
	use := findIndexUse(t, sourcePackages(t)...)
	if len(use.values) != 1 || len(use.vars) != 1 {
		t.Errorf("the request path makes %d indexes, want the handler's one: %s", len(use.values), strings.Join(use.values, "; "))
	}
	if len(use.reads) != 1 {
		t.Errorf("an index is read at %d places, want one: %s", len(use.reads), strings.Join(use.reads, "; "))
	}
	if len(use.params) == 0 {
		t.Fatal("no stage takes the index: this test no longer sees it")
	}

	dirs := []string{".", "../dedup", "../coalesce", "../contextbudget", "../compiler", "../templates"}
	cmd := exec.Command(goTool, append([]string{"build", "-gcflags=-m", "-o", os.DevNull}, dirs...)...)
	cmd.Env = append(os.Environ(), "GOFLAGS=") // -trimpath would change the paths matched below
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build -gcflags=-m: %v\n%s", err, out)
	}
	diag := regexp.MustCompile(`(?m)^(\S+?:\d+:\d+): (moved to heap|leaking param|leaking param content): (\S+)$`)
	seen := 0
	for _, d := range diag.FindAllStringSubmatch(string(out), -1) {
		where := filepath.Clean(d[1]) + " " + d[3]
		_, isVar := use.vars[where]
		_, isParam := use.params[where]
		switch {
		case d[2] == "moved to heap" && isVar:
			t.Errorf("the index is on the heap: %s", d[0])
		case d[2] == "leaking param" && isParam:
			t.Errorf("a stage keeps the index: %s", d[0])
		case d[2] == "leaking param content" && isParam:
			seen++
		}
	}
	// An analysis that said nothing would pass whatever the code does.
	if seen == 0 {
		t.Fatalf("the compiler said nothing of the index (want its pointer to be seen not to leak):\n%.2000s", out)
	}
	for _, kept := range []*regexp.Regexp{
		regexp.MustCompile(`(?m)^.*new\(bodyindex\.Index\) escapes.*$`),
		regexp.MustCompile(`(?m)^.*bodyindex\.Index\{.*\} escapes.*$`),
	} {
		if lines := kept.FindAllString(string(out), -1); len(lines) > 0 {
			t.Errorf("an index is kept: %s", strings.Join(lines, "\n"))
		}
	}
}
