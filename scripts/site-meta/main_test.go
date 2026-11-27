package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const urlset = `<?xml version="1.0" encoding="utf-8"?>` +
	`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
	`<url><loc>https://peers.zhanwei.wang/</loc><lastmod>2026-11-26T00:00:00+00:00</lastmod></url>` +
	`</urlset>`

const sitemapIndex = `<?xml version="1.0" encoding="utf-8"?>` +
	`<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
	`<sitemap><loc>https://peers.zhanwei.wang/en/sitemap.xml</loc></sitemap>` +
	`<sitemap><loc>https://peers.zhanwei.wang/zh/sitemap.xml</loc></sitemap>` +
	`</sitemapindex>`

const blocksSeparator = "\n\n---\n\n"

const rootBase = "https://peers.zhanwei.wang/"

// pageSpec is the head and version markup of a generated page.
type pageSpec struct {
	description         string
	ogDescription       string
	ogType              string
	twitterDescription  string
	itempropDescription string
	robots              string
	canonical           string
	markdownAlt         string
	hreflangs           []string
	ogURL               string
	jsonLD              string
	selector            bool
	banner              bool
}

func renderPage(s pageSpec) string {
	var b strings.Builder
	b.WriteString("<!doctype html><html><head>")
	fmt.Fprintf(&b, `<meta name="description" content="%s">`, s.description)
	fmt.Fprintf(&b, `<meta property="og:description" content="%s">`, s.ogDescription)
	fmt.Fprintf(&b, `<meta property="og:type" content="%s">`, s.ogType)
	fmt.Fprintf(&b, `<meta name="twitter:description" content="%s">`, s.twitterDescription)
	fmt.Fprintf(&b, `<meta itemprop="description" content="%s">`, s.itempropDescription)
	fmt.Fprintf(&b, `<meta name="robots" content="%s">`, s.robots)
	if s.canonical != "" {
		fmt.Fprintf(&b, `<link rel="canonical" href="%s">`, s.canonical)
	}
	if s.markdownAlt != "" {
		fmt.Fprintf(&b, `<link rel="alternate" type="text/markdown" href="%s">`, s.markdownAlt)
	}
	for _, h := range s.hreflangs {
		fmt.Fprintf(&b, `<link rel="alternate" hreflang="%s" href="https://peers.zhanwei.wang/%s/">`, h, h)
	}
	if s.ogURL != "" {
		fmt.Fprintf(&b, `<meta property="og:url" content="%s">`, s.ogURL)
	}
	if s.jsonLD != "" {
		fmt.Fprintf(&b, `<script type="application/ld+json">%s</script>`, s.jsonLD)
	}
	b.WriteString("</head><body>")
	if s.selector {
		b.WriteString(`<nav data-version-selector></nav>`)
	}
	if s.banner {
		b.WriteString(`<div data-version-banner></div>`)
	}
	b.WriteString("</body></html>")
	return b.String()
}

func classBase(cls class) string {
	switch cls {
	case classLatest:
		return rootBase
	case classLatestCopy:
		return rootBase + "v0.6.2/"
	case classOlder:
		return rootBase + "v0.6.1/"
	case classDev:
		return rootBase + "dev/"
	}
	return ""
}

func homeJSONLD(cls class) string {
	if cls == classDev {
		return `{"@context":"https://schema.org","@type":"SoftwareApplication","name":"peers"}`
	}
	return `{"@context":"https://schema.org","@type":"SoftwareApplication","name":"peers","softwareVersion":"0.6.2"}`
}

func hreflangs(cls class) []string {
	if cls == classLatest {
		return []string{"en", "zh-CN", "x-default"}
	}
	return nil
}

func validPage(rel string, cls class) pageSpec {
	alt := classBase(cls) + pageDir(rel) + "index.md"
	canonical := strings.TrimSuffix(alt, "index.md")
	if cls == classLatestCopy {
		canonical = rootBase + pageDir(rel)
	}
	s := pageSpec{
		description:         "Short summary.",
		ogDescription:       "Short summary.",
		ogType:              "website",
		twitterDescription:  "Short summary.",
		itempropDescription: "Short summary.",
		robots:              cls.robots(),
		canonical:           canonical,
		markdownAlt:         alt,
		hreflangs:           hreflangs(cls),
		ogURL:               canonical,
		selector:            true,
		banner:              cls.banner(),
	}
	if rel == "index.html" || rel == "zh/index.html" {
		s.jsonLD = homeJSONLD(cls)
	}
	return s
}

func versionLine(cls class) string {
	switch cls {
	case classLatest, classLatestCopy:
		return "Version: v0.6.2 (latest release)"
	case classOlder:
		return "Version: v0.6.1 (superseded; latest release v0.6.2 at https://peers.zhanwei.wang/docs/quickstart/)"
	case classDev:
		return "Version: development (unreleased; latest release v0.6.2 at https://peers.zhanwei.wang/docs/quickstart/)"
	}
	return ""
}

func siteIndex(cls class) string {
	return fmt.Sprintf("# peers\n\n> A cross-host messaging system.\n\n%s\n\n## Documentation\n\n- [Quick start](https://peers.zhanwei.wang/docs/quickstart/): Start here.", versionLine(cls))
}

func pageMarkdown(cls class, title, dir string) string {
	return fmt.Sprintf("# %s\n\nSource: %s%s/index.md\n%s\n\nBody.", title, classBase(cls), dir, versionLine(cls))
}

func redirectPage() string {
	return `<!doctype html><html lang=en><head><title>https://peers.zhanwei.wang/</title>` +
		`<link rel=canonical href=https://peers.zhanwei.wang/><meta charset=utf-8>` +
		`<meta http-equiv=refresh content="0; url=https://peers.zhanwei.wang/"></head></html>`
}

// validFiles returns a build tree that passes every check for the class.
func validFiles(cls class) map[string]string {
	files := map[string]string{
		"en/index.html": redirectPage(),
	}
	for _, rel := range []string{"index.html", "zh/index.html", "docs/quickstart/index.html", "zh/docs/quickstart/index.html"} {
		files[rel] = renderPage(validPage(rel, cls))
	}
	files["index.md"] = siteIndex(cls)
	files["en/index.md"] = siteIndex(cls)
	files["zh/index.md"] = siteIndex(cls)
	files["docs/quickstart/index.md"] = pageMarkdown(cls, "Quick start", "docs/quickstart")
	files["zh/docs/quickstart/index.md"] = pageMarkdown(cls, "快速开始", "zh/docs/quickstart")
	files["llms.txt"] = siteIndex(cls)
	files["llms-full.txt"] = pageMarkdown(cls, "Quick start", "docs/quickstart") + blocksSeparator + pageMarkdown(cls, "FAQ", "docs/faq")
	files["zh/llms.txt"] = siteIndex(cls)
	files["zh/llms-full.txt"] = pageMarkdown(cls, "快速开始", "zh/docs/quickstart")
	if cls == classLatest {
		files["sitemap.xml"] = sitemapIndex
		files["en/sitemap.xml"] = urlset
		files["zh/sitemap.xml"] = urlset
	}
	return files
}

func canonicalBaseFor(cls class) string {
	if cls == classLatestCopy {
		return rootBase
	}
	return ""
}

// writeTree writes a generated tree into a temporary directory and returns its
// root.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("create directory for %s: %v", rel, err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	return root
}

// checkSite writes a generated tree into a temporary directory and runs check.
func checkSite(t *testing.T, cls class, files map[string]string) []string {
	t.Helper()
	root := writeTree(t, files)
	problems, err := check(root, cls, canonicalBaseFor(cls))
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

func withCanonical(s pageSpec, canonical string) pageSpec {
	s.canonical = canonical
	s.ogURL = canonical
	return s
}

// TestValidSitePassesEachClass checks that a tree with the right signals for
// its class produces no problem.
func TestValidSitePassesEachClass(t *testing.T) {
	for _, cls := range []class{classLatest, classLatestCopy, classOlder, classDev} {
		t.Run(string(cls), func(t *testing.T) {
			wantProblems(t, checkSite(t, cls, validFiles(cls)))
		})
	}
}

// TestWhitespaceAroundOpenGraphValues checks that whitespace around the
// description and og:type values is reported.
func TestWhitespaceAroundOpenGraphValues(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("index.html", classLatest)
	s.ogDescription = "\n    Short summary.\n  "
	s.ogType = "\n    website\n  "
	files["index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files),
		`index.html: meta property="og:description" has surrounding whitespace`,
		`index.html: meta property="og:type" has surrounding whitespace`,
	)
}

// TestTwitterDescriptionFromPageSummary checks that a Twitter or schema.org
// description that differs from the page description is reported.
func TestTwitterDescriptionFromPageSummary(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.twitterDescription = "Quick start curl -fsSL https://example.com/install.sh | sh"
	s.itempropDescription = s.twitterDescription
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files),
		`docs/quickstart/index.html: meta name="twitter:description" differs from the description`,
		`docs/quickstart/index.html: meta itemprop="description" differs from the description`,
	)
}

// TestMissingDescriptionAndType checks that an empty description and og:type
// are reported.
func TestMissingDescriptionAndType(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.description = ""
	s.ogDescription = ""
	s.twitterDescription = ""
	s.itempropDescription = ""
	s.ogType = ""
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files),
		`docs/quickstart/index.html: meta name="description" is missing or empty`,
		`docs/quickstart/index.html: meta property="og:type" is missing or empty`,
	)
}

// TestAliasRedirectPageIsSkippedEachClass checks that the redirect page Hugo
// writes for an alias is skipped for every class.
func TestAliasRedirectPageIsSkippedEachClass(t *testing.T) {
	for _, cls := range []class{classLatest, classLatestCopy, classOlder, classDev} {
		t.Run(string(cls), func(t *testing.T) {
			files := validFiles(cls)
			files["en/index.html"] = redirectPage()
			wantProblems(t, checkSite(t, cls, files))
		})
	}
}

// TestSitemapURLWithoutLastmod checks that a sitemap URL without a lastmod is
// reported.
func TestSitemapURLWithoutLastmod(t *testing.T) {
	files := validFiles(classLatest)
	files["en/sitemap.xml"] = `<?xml version="1.0" encoding="utf-8"?>` +
		`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
		`<url><loc>https://peers.zhanwei.wang/docs/</loc></url></urlset>`
	wantProblems(t, checkSite(t, classLatest, files), `en/sitemap.xml: no lastmod for https://peers.zhanwei.wang/docs/`)
}

// TestRobotsMetaMatchesClass checks that the wrong robots value is reported
// for each class.
func TestRobotsMetaMatchesClass(t *testing.T) {
	cases := []struct {
		cls   class
		wrong string
	}{
		{classLatest, "noindex, follow"},
		{classLatestCopy, "noindex, follow"},
		{classOlder, "index, follow"},
		{classDev, "index, follow"},
	}
	for _, tc := range cases {
		t.Run(string(tc.cls), func(t *testing.T) {
			files := validFiles(tc.cls)
			s := validPage("docs/quickstart/index.html", tc.cls)
			s.robots = tc.wrong
			files["docs/quickstart/index.html"] = renderPage(s)
			wantProblems(t, checkSite(t, tc.cls, files), `docs/quickstart/index.html: meta name="robots"`)
		})
	}
}

// TestCrossPageCanonicalOnOlder checks that an older page canonicalising to a
// different page is reported.
func TestCrossPageCanonicalOnOlder(t *testing.T) {
	files := validFiles(classOlder)
	s := withCanonical(validPage("docs/quickstart/index.html", classOlder), classBase(classOlder)+"docs/faq/")
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classOlder, files), `docs/quickstart/index.html: link rel="canonical"`)
}

// TestRootCanonicalOnOlder checks that an older page canonicalising to the
// same path under the root is reported.
func TestRootCanonicalOnOlder(t *testing.T) {
	files := validFiles(classOlder)
	s := withCanonical(validPage("docs/quickstart/index.html", classOlder), rootBase+"docs/quickstart/")
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classOlder, files), `docs/quickstart/index.html: link rel="canonical"`)
}

// TestLatestCopyCanonicalIgnoresOwnPermalink checks that a latest-copy page
// canonicalising to its own versioned URL is reported.
func TestLatestCopyCanonicalIgnoresOwnPermalink(t *testing.T) {
	files := validFiles(classLatestCopy)
	s := withCanonical(validPage("docs/quickstart/index.html", classLatestCopy), classBase(classLatestCopy)+"docs/quickstart/")
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatestCopy, files), `docs/quickstart/index.html: link rel="canonical"`)
}

// TestOGURLDiffersFromCanonical checks that an og:url different from the
// canonical is reported.
func TestOGURLDiffersFromCanonical(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.ogURL = "https://example.com/other/"
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: meta property="og:url" differs from the canonical`)
}

// TestMissingXDefaultOnLatest checks that a latest page without x-default is
// reported.
func TestMissingXDefaultOnLatest(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.hreflangs = []string{"en", "zh-CN"}
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: link rel="alternate" hreflang="x-default" is missing`)
}

// TestHreflangOnDev checks that any hreflang link on a dev page is reported.
func TestHreflangOnDev(t *testing.T) {
	files := validFiles(classDev)
	s := validPage("docs/quickstart/index.html", classDev)
	s.hreflangs = []string{"en"}
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classDev, files), `docs/quickstart/index.html: hreflang links are not allowed for class dev`)
}

// TestMissingSelectorOnDocsPage checks that a page without the version
// selector is reported.
func TestMissingSelectorOnDocsPage(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.selector = false
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: no element with data-version-selector`)
}

// TestBannerMissingOnOlderAndDev checks that a page without the banner on the
// classes that require it is reported.
func TestBannerMissingOnOlderAndDev(t *testing.T) {
	for _, cls := range []class{classOlder, classDev} {
		t.Run(string(cls), func(t *testing.T) {
			files := validFiles(cls)
			s := validPage("docs/quickstart/index.html", cls)
			s.banner = false
			files["docs/quickstart/index.html"] = renderPage(s)
			wantProblems(t, checkSite(t, cls, files), `docs/quickstart/index.html: no element with data-version-banner`)
		})
	}
}

// TestBannerPresentOnLatestAndLatestCopy checks that the banner on the classes
// that forbid it is reported.
func TestBannerPresentOnLatestAndLatestCopy(t *testing.T) {
	for _, cls := range []class{classLatest, classLatestCopy} {
		t.Run(string(cls), func(t *testing.T) {
			files := validFiles(cls)
			s := validPage("docs/quickstart/index.html", cls)
			s.banner = true
			files["docs/quickstart/index.html"] = renderPage(s)
			wantProblems(t, checkSite(t, cls, files), `docs/quickstart/index.html: element with data-version-banner is not allowed for class `+string(cls))
		})
	}
}

// TestMissingVersionLineInIndex checks that an index.md whose line after
// Source is not a Version line is reported.
func TestMissingVersionLineInIndex(t *testing.T) {
	files := validFiles(classLatest)
	files["docs/quickstart/index.md"] = "# Quick start\n\nSource: https://peers.zhanwei.wang/docs/quickstart/index.md\n\nBody."
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.md: the line after "Source:" is not a Version line`)
}

// TestMissingVersionLineInLLMS checks that an llms.txt without a Version line
// after the summary is reported.
func TestMissingVersionLineInLLMS(t *testing.T) {
	files := validFiles(classLatest)
	files["llms.txt"] = "# peers\n\n> A cross-host messaging system.\n\n## Documentation\n\n- [Quick start](https://peers.zhanwei.wang/docs/quickstart/): Start here."
	wantProblems(t, checkSite(t, classLatest, files), `llms.txt: the first line after the "> " summary is not a Version line`)
}

// TestMissingVersionLineInLLMSFullBlock checks that a page block of
// llms-full.txt without a Version line is reported.
func TestMissingVersionLineInLLMSFullBlock(t *testing.T) {
	files := validFiles(classLatest)
	files["llms-full.txt"] = pageMarkdown(classLatest, "Quick start", "docs/quickstart") + blocksSeparator +
		"# FAQ\n\nSource: https://peers.zhanwei.wang/docs/faq/index.md\n\nBody."
	wantProblems(t, checkSite(t, classLatest, files), `llms-full.txt: the line after "Source:" is not a Version line`)
}

// TestVersionLineOneLineTooLate checks that a Version line separated from
// Source by a blank line is reported.
func TestVersionLineOneLineTooLate(t *testing.T) {
	files := validFiles(classLatest)
	files["docs/quickstart/index.md"] = "# Quick start\n\nSource: https://peers.zhanwei.wang/docs/quickstart/index.md\n\n" + versionLine(classLatest) + "\n\nBody."
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.md: the line after "Source:" is not a Version line`)
}

// TestLatestVersionFormOnOlder checks that the latest wording on an older page
// is reported.
func TestLatestVersionFormOnOlder(t *testing.T) {
	files := validFiles(classOlder)
	files["docs/quickstart/index.md"] = "# Quick start\n\nSource: https://peers.zhanwei.wang/v0.6.1/docs/quickstart/index.md\nVersion: v0.6.1 (latest release)\n\nBody."
	wantProblems(t, checkSite(t, classOlder, files), `docs/quickstart/index.md: Version line is not the older form`)
}

// TestTranslatedVersionLineInChinese checks that a translated Version line in
// a zh rendition is reported.
func TestTranslatedVersionLineInChinese(t *testing.T) {
	files := validFiles(classLatest)
	files["zh/docs/quickstart/index.md"] = "# 快速开始\n\nSource: https://peers.zhanwei.wang/zh/docs/quickstart/index.md\nVersion: v0.6.2（最新版本）\n\n正文。"
	wantProblems(t, checkSite(t, classLatest, files), `zh/docs/quickstart/index.md: Version line is not the latest form`)
}

// TestSoftwareVersionMissingOnLatest checks that the home page JSON-LD without
// softwareVersion is reported on latest.
func TestSoftwareVersionMissingOnLatest(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("index.html", classLatest)
	s.jsonLD = `{"@context":"https://schema.org","@type":"SoftwareApplication","name":"peers"}`
	files["index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `index.html: JSON-LD SoftwareApplication has no softwareVersion`)
}

// TestSoftwareVersionPresentOnDev checks that softwareVersion on the dev home
// page is reported.
func TestSoftwareVersionPresentOnDev(t *testing.T) {
	files := validFiles(classDev)
	s := validPage("index.html", classDev)
	s.jsonLD = homeJSONLD(classLatest)
	files["index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classDev, files), `index.html: JSON-LD SoftwareApplication carries softwareVersion for class dev`)
}

// TestSitemapPresentForOlder checks that a sitemap in a non-latest build is
// reported.
func TestSitemapPresentForOlder(t *testing.T) {
	files := validFiles(classOlder)
	files["sitemap.xml"] = sitemapIndex
	wantProblems(t, checkSite(t, classOlder, files), `sitemap.xml: sitemap.xml must not exist for class older`)
}

// TestSitemapMissingForLatest checks that a latest build without a sitemap is
// reported.
func TestSitemapMissingForLatest(t *testing.T) {
	files := validFiles(classLatest)
	delete(files, "sitemap.xml")
	delete(files, "en/sitemap.xml")
	delete(files, "zh/sitemap.xml")
	wantProblems(t, checkSite(t, classLatest, files), `sitemap.xml: missing for class latest`)
}

// TestMissingMarkdownAlternate checks that a page without the text/markdown
// alternate link is reported.
func TestMissingMarkdownAlternate(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.markdownAlt = ""
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: no text/markdown alternate link`)
}

// TestMissingHomeJSONLD checks that each home page without a
// SoftwareApplication is reported.
func TestMissingHomeJSONLD(t *testing.T) {
	for _, rel := range []string{"index.html", "zh/index.html"} {
		t.Run(rel, func(t *testing.T) {
			files := validFiles(classLatest)
			s := validPage(rel, classLatest)
			s.jsonLD = ""
			files[rel] = renderPage(s)
			wantProblems(t, checkSite(t, classLatest, files), rel+": home page JSON-LD SoftwareApplication is missing")
		})
	}
}

// TestDevLatestVersionForm checks that the latest Version line on a dev page
// is reported.
func TestDevLatestVersionForm(t *testing.T) {
	files := validFiles(classDev)
	files["docs/quickstart/index.md"] = "# Quick start\n\nSource: https://peers.zhanwei.wang/dev/docs/quickstart/index.md\nVersion: v0.6.2 (latest release)\n\nBody."
	wantProblems(t, checkSite(t, classDev, files), `docs/quickstart/index.md: Version line is not the dev form`)
}

// TestRunExitCodes checks that a clean tree exits 0 and a tree with a
// violation exits 1.
func TestRunExitCodes(t *testing.T) {
	t.Run("clean tree", func(t *testing.T) {
		root := writeTree(t, validFiles(classLatest))
		var stderr bytes.Buffer
		if code := run([]string{"-public", root}, &stderr); code != 0 {
			t.Fatalf("run() = %d, want 0; stderr:\n%s", code, stderr.String())
		}
	})
	t.Run("violation", func(t *testing.T) {
		files := validFiles(classLatest)
		s := validPage("docs/quickstart/index.html", classLatest)
		s.robots = "noindex, follow"
		files["docs/quickstart/index.html"] = renderPage(s)
		root := writeTree(t, files)
		var stderr bytes.Buffer
		if code := run([]string{"-public", root}, &stderr); code != 1 {
			t.Fatalf("run() = %d, want 1; stderr:\n%s", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "robots") {
			t.Errorf("stderr does not name the robots violation:\n%s", stderr.String())
		}
	})
}

// TestCanonicalBaseWithoutTrailingSlash checks that -canonical-base without a
// trailing slash is accepted and normalised.
func TestCanonicalBaseWithoutTrailingSlash(t *testing.T) {
	root := writeTree(t, validFiles(classLatestCopy))
	var stderr bytes.Buffer
	args := []string{"-public", root, "-class", "latest-copy", "-canonical-base", "https://peers.zhanwei.wang"}
	if code := run(args, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr:\n%s", code, stderr.String())
	}
}

// TestDuplicateRobotsMeta checks that a second robots meta is reported.
func TestDuplicateRobotsMeta(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	page := renderPage(s)
	marker := fmt.Sprintf(`<meta name="robots" content="%s">`, s.robots)
	page = strings.Replace(page, marker, marker+marker, 1)
	files["docs/quickstart/index.html"] = page
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: meta name="robots" appears more than once`)
}

// TestDuplicateCanonical checks that a second canonical link is reported.
func TestDuplicateCanonical(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	page := renderPage(s)
	marker := fmt.Sprintf(`<link rel="canonical" href="%s">`, s.canonical)
	page = strings.Replace(page, marker, marker+marker, 1)
	files["docs/quickstart/index.html"] = page
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: link rel="canonical" appears more than once`)
}

// TestInvalidJSONLD checks that a malformed JSON-LD block is reported.
func TestInvalidJSONLD(t *testing.T) {
	files := validFiles(classLatest)
	s := validPage("docs/quickstart/index.html", classLatest)
	s.jsonLD = `{"@type":`
	files["docs/quickstart/index.html"] = renderPage(s)
	wantProblems(t, checkSite(t, classLatest, files), `docs/quickstart/index.html: JSON-LD block is not valid JSON`)
}

// TestMissingLLMSFiles checks that a home directory without llms.txt or
// llms-full.txt is reported.
func TestMissingLLMSFiles(t *testing.T) {
	cases := []struct {
		rel  string
		want string
	}{
		{"llms.txt", "llms.txt: missing"},
		{"llms-full.txt", "llms-full.txt: missing"},
		{"zh/llms.txt", "zh/llms.txt: missing"},
	}
	for _, tc := range cases {
		t.Run(tc.rel, func(t *testing.T) {
			files := validFiles(classLatest)
			delete(files, tc.rel)
			wantProblems(t, checkSite(t, classLatest, files), tc.want)
		})
	}
}

// TestLLMSFullWithoutSourceBlock checks that an llms-full.txt with no Source
// line is reported.
func TestLLMSFullWithoutSourceBlock(t *testing.T) {
	files := validFiles(classLatest)
	files["llms-full.txt"] = "# peers\n\nA page block without a Source line."
	wantProblems(t, checkSite(t, classLatest, files), `llms-full.txt: no "Source:" block`)
}

// TestBadFlagsExitTwo checks that bad flag combinations exit with code 2.
func TestBadFlagsExitTwo(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
	}{
		{"canonical base missing for latest-copy", []string{"-public", dir, "-class", "latest-copy"}},
		{"canonical base given for older", []string{"-public", dir, "-class", "older", "-canonical-base", rootBase}},
		{"unknown class", []string{"-public", dir, "-class", "beta"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run(tc.args, io.Discard); code != 2 {
				t.Fatalf("run(%v) = %d, want 2", tc.args, code)
			}
		})
	}
}
