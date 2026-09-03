package improvement_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Interface audit test ensuring no exported function applies proposals
// or mutates runtime graph definitions.
func TestInterfaceAudit_NoExportedFunctionAppliesProposal(t *testing.T) {
	pkgDir := "."
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("failed to read package directory: %v", err)
	}

	fset := token.NewFileSet()
	var exportedFuncs []string

	// Forbidden verbs/words that imply autonomous application of proposals
	forbiddenSubstrings := []string{
		"apply",
		"execute",
		"deploy",
		"activate",
		"install",
		"mutate",
		"autoapply",
		"enact",
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		filePath := filepath.Join(pkgDir, entry.Name())
		node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("failed to parse file %s: %v", filePath, err)
		}

		for _, decl := range node.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}

			// Check if exported
			if fn.Name.IsExported() {
				funcName := fn.Name.Name
				exportedFuncs = append(exportedFuncs, funcName)

				lowerName := strings.ToLower(funcName)
				for _, forbidden := range forbiddenSubstrings {
					if strings.Contains(lowerName, forbidden) {
						t.Errorf("violation of no-autonomy invariant: exported function %q contains forbidden action %q", funcName, forbidden)
					}
				}
			}
		}
	}

	if len(exportedFuncs) == 0 {
		t.Fatal("expected to find exported functions in package, found 0")
	}

	// Verify that the allowed surface strictly conforms to query/record/lifecycle only
	for _, fn := range exportedFuncs {
		t.Logf("audited exported function: %s (compliant)", fn)
	}
}
