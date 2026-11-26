package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const urlset = `<?xml version="1.0" encoding="utf-8"?>` +
	`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
	`<url><loc>https://example.com/</loc><lastmod>2026-11-26T00:00:00+00:00</lastmod></url>` +
	`</urlset>`

// page returns a generated page whose head holds the given tags.
func page(head string) string {
	return "<!doctype html><html><head>" + head + "</head><body>" +
		`<meta name="description" content="body tags do not count">` +
		"</body></html>"
}

const goodHead = `<meta name="description" content="Short summary.">` +
	`<meta property="og:description" content="Short summary.">` +
	`<meta property="og:type" content="website">` +
	`<meta name="twitter:description" content="Short summary.">` +
	`<meta itemprop="description" content="Short summary.">`

// checkSite writes a generated tree into a temporary directory and runs check.
func checkSite(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create directory for %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	problems, err := check(root)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	return problems
}

// wantProblems fails unless every fragment appears in some problem and the
// number of problems matches.
func wantProblems(t *testing.T, problems []string, fragments ...string) {
	t.Helper()
	if len(problems) != len(fragments) {
		t.Fatalf("got %d problems, want %d:\n%s", len(problems), len(fragments), strings.Join(problems, "\n"))
	}
	for _, fragment := range fragments {
		found := false
		for _, p := range problems {
			if strings.Contains(p, fragment) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no problem contains %q:\n%s", fragment, strings.Join(problems, "\n"))
		}
	}
}

func TestConsistentMetadataPasses(t *testing.T) {
	problems := checkSite(t, map[string]string{
		"index.html":     page(goodHead),
		"en/sitemap.xml": urlset,
	})
	wantProblems(t, problems)
}

func TestWhitespaceAroundOpenGraphValues(t *testing.T) {
	head := `<meta name="description" content="Short summary.">` +
		"<meta property=\"og:description\" content=\"\n    Short summary.\n  \">" +
		"<meta property=\"og:type\" content=\"\n    website\n  \">"
	problems := checkSite(t, map[string]string{
		"index.html":     page(head),
		"en/sitemap.xml": urlset,
	})
	wantProblems(t, problems,
		`index.html: meta property="og:description" has surrounding whitespace`,
		`index.html: meta property="og:type" has surrounding whitespace`,
	)
}

func TestTwitterDescriptionFromPageSummary(t *testing.T) {
	head := `<meta name="description" content="Short summary.">` +
		`<meta property="og:type" content="website">` +
		`<meta name="twitter:description" content="Quick start curl -fsSL https://example.com/install.sh | sh">` +
		`<meta itemprop="description" content="Quick start curl -fsSL https://example.com/install.sh | sh">`
	problems := checkSite(t, map[string]string{
		"docs/index.html": page(head),
		"en/sitemap.xml":  urlset,
	})
	wantProblems(t, problems,
		`docs/index.html: meta name="twitter:description" differs from the description`,
		`docs/index.html: meta itemprop="description" differs from the description`,
	)
}

func TestMissingDescriptionAndType(t *testing.T) {
	problems := checkSite(t, map[string]string{
		"index.html":     page(""),
		"en/sitemap.xml": urlset,
	})
	wantProblems(t, problems,
		`meta name="description" is missing or empty`,
		`meta property="og:type" is missing or empty`,
	)
}

func TestAliasRedirectPageIsSkipped(t *testing.T) {
	problems := checkSite(t, map[string]string{
		"index.html": page(goodHead),
		"en/index.html": `<!doctype html><html lang=en><head><title>https://example.com/</title>` +
			`<link rel=canonical href=https://example.com/><meta charset=utf-8>` +
			`<meta http-equiv=refresh content="0; url=https://example.com/"></head></html>`,
		"en/sitemap.xml": urlset,
	})
	wantProblems(t, problems)
}

func TestSitemapURLWithoutLastmod(t *testing.T) {
	problems := checkSite(t, map[string]string{
		"index.html": page(goodHead),
		"sitemap.xml": `<?xml version="1.0" encoding="utf-8"?>` +
			`<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
			`<sitemap><loc>https://example.com/en/sitemap.xml</loc></sitemap></sitemapindex>`,
		"en/sitemap.xml": `<?xml version="1.0" encoding="utf-8"?>` +
			`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
			`<url><loc>https://example.com/docs/</loc></url></urlset>`,
	})
	wantProblems(t, problems, `en/sitemap.xml: no lastmod for https://example.com/docs/`)
}
