// Command site-meta checks the search and sharing metadata of the built
// website: every index.html carries a consistent description, Open Graph,
// Twitter card and schema.org summary, every sitemap URL a lastmod, and a
// redirect page is skipped. Given the build's version class, it also checks
// the robots, canonical, og:url and hreflang signals, the version selector and
// banner, the Markdown and llms Version lines, and the presence of a sitemap.
// Every home page carries a SoftwareApplication JSON-LD object with a
// softwareVersion that a dev build omits, every JSON-LD block is valid JSON,
// the robots meta and canonical link appear once, each home directory has an
// llms.txt and an llms-full.txt, and every llms-full.txt has at least one
// Source block.
package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/net/html"
)

// class is the version class of one build.
type class string

const (
	classLatest     class = "latest"
	classLatestCopy class = "latest-copy"
	classOlder      class = "older"
	classDev        class = "dev"
)

func (c class) valid() bool {
	switch c {
	case classLatest, classLatestCopy, classOlder, classDev:
		return true
	}
	return false
}

func (c class) robots() string {
	if c == classLatest || c == classLatestCopy {
		return "index, follow"
	}
	return "noindex, follow"
}

func (c class) banner() bool {
	return c == classOlder || c == classDev
}

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

// run parses the flags and checks the tree, returning the process exit code: 0
// when clean, 1 on violations and 2 on bad flags or a failed read.
func run(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("site-meta", flag.ContinueOnError)
	flags.SetOutput(stderr)
	publicDir := flags.String("public", "docs/site/public", "generated website root")
	className := flags.String("class", string(classLatest), "version class of the build: latest, latest-copy, older or dev")
	canonicalBase := flags.String("canonical-base", "", "root base URL with a trailing slash; required for latest-copy and rejected otherwise")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	cls := class(*className)
	if !cls.valid() {
		_, _ = fmt.Fprintf(stderr, "site-meta: unknown class %q; want latest, latest-copy, older or dev\n", *className)
		return 2
	}
	if cls == classLatestCopy {
		if *canonicalBase == "" {
			_, _ = fmt.Fprintln(stderr, "site-meta: -canonical-base is required for -class latest-copy")
			return 2
		}
		*canonicalBase = strings.TrimSuffix(*canonicalBase, "/") + "/"
	} else if *canonicalBase != "" {
		_, _ = fmt.Fprintln(stderr, "site-meta: -canonical-base is only valid for -class latest-copy")
		return 2
	}

	problems, err := check(*publicDir, cls, *canonicalBase)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "site-meta: %v\n", err)
		return 2
	}
	for _, problem := range problems {
		_, _ = fmt.Fprintln(stderr, "site-meta: "+problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// check walks the generated tree and returns one message per problem.
func check(publicDir string, cls class, canonicalBase string) ([]string, error) {
	var pages, sitemaps, markdowns, llms []string
	err := filepath.WalkDir(publicDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		switch d.Name() {
		case "index.html":
			pages = append(pages, p)
		case "sitemap.xml":
			sitemaps = append(sitemaps, p)
		case "index.md":
			markdowns = append(markdowns, p)
		case "llms.txt", "llms-full.txt":
			llms = append(llms, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, fmt.Errorf("no index.html under %s", publicDir)
	}
	sort.Strings(pages)
	sort.Strings(sitemaps)
	sort.Strings(markdowns)
	sort.Strings(llms)

	var problems []string
	homes, err := homeDirs(publicDir, markdowns)
	if err != nil {
		return nil, err
	}
	llmsSet := map[string]bool{}
	for _, p := range llms {
		llmsSet[relSlash(publicDir, p)] = true
	}
	for _, p := range pages {
		rel := relSlash(publicDir, p)
		pp, err := parsePage(p)
		if err != nil {
			return nil, err
		}
		found, app := checkPage(pp, cls, canonicalBase, rel)
		for _, problem := range found {
			problems = append(problems, rel+": "+problem)
		}
		if pp.redirect {
			continue
		}
		dir := pageDirKey(rel)
		if !homes[dir] {
			continue
		}
		if !app {
			problems = append(problems, rel+": home page JSON-LD SoftwareApplication is missing")
		}
		for _, name := range []string{"llms.txt", "llms-full.txt"} {
			relPath := name
			if dir != "" {
				relPath = dir + "/" + name
			}
			if !llmsSet[relPath] {
				problems = append(problems, relPath+": missing")
			}
		}
	}

	switch cls {
	case classLatest:
		if len(sitemaps) == 0 {
			problems = append(problems, "sitemap.xml: missing for class latest")
		}
		for _, p := range sitemaps {
			found, err := checkSitemap(p)
			if err != nil {
				return nil, err
			}
			for _, problem := range found {
				problems = append(problems, relSlash(publicDir, p)+": "+problem)
			}
		}
	default:
		for _, p := range sitemaps {
			problems = append(problems, relSlash(publicDir, p)+": sitemap.xml must not exist for class "+string(cls))
		}
	}

	for _, p := range markdowns {
		found, err := checkVersionFile(p, kindIndex, cls)
		if err != nil {
			return nil, err
		}
		for _, problem := range found {
			problems = append(problems, relSlash(publicDir, p)+": "+problem)
		}
	}
	for _, p := range llms {
		kind := kindLLMS
		if filepath.Base(p) == "llms-full.txt" {
			kind = kindLLMSFull
		}
		found, err := checkVersionFile(p, kind, cls)
		if err != nil {
			return nil, err
		}
		for _, problem := range found {
			problems = append(problems, relSlash(publicDir, p)+": "+problem)
		}
	}
	return problems, nil
}

func relSlash(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return filepath.ToSlash(p)
	}
	return filepath.ToSlash(rel)
}

// pageDir returns the page's directory below the build root with a trailing
// slash, which is the path its URL carries.
func pageDir(rel string) string {
	dir := path.Dir(rel)
	if dir == "." {
		return ""
	}
	return dir + "/"
}

// pageDirKey returns the page's directory below the build root without a
// trailing slash; the build root is "".
func pageDirKey(rel string) string {
	return strings.TrimSuffix(pageDir(rel), "/")
}

// homeDirs returns the directories whose index.md is a home page's Markdown
// rendition. That file starts with the "> " summary and carries no Source
// line; every other index.md is a documentation page.
func homeDirs(publicDir string, markdowns []string) (map[string]bool, error) {
	dirs := map[string]bool{}
	for _, p := range markdowns {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		if firstSourceLine(splitLines(string(data))) >= 0 {
			continue
		}
		dirs[pageDirKey(relSlash(publicDir, p))] = true
	}
	return dirs, nil
}

// metaTag names one meta tag by the attribute that identifies it.
type metaTag struct {
	attr, value string
}

func (m metaTag) String() string {
	return fmt.Sprintf("meta %s=%q", m.attr, m.value)
}

var (
	description        = metaTag{"name", "description"}
	ogType             = metaTag{"property", "og:type"}
	robotsTag          = metaTag{"name", "robots"}
	ogURLTag           = metaTag{"property", "og:url"}
	sameAsDescriptions = []metaTag{
		{"property", "og:description"},
		{"name", "twitter:description"},
		{"itemprop", "description"},
	}
)

// parsedPage is the metadata and version markup read from one generated page.
type parsedPage struct {
	metas              map[metaTag]string
	metaCounts         map[metaTag]int
	canonical          string
	hasCanonical       bool
	duplicateCanonical bool
	mdAlt              string
	hasMDAlt           bool
	hreflangs          []string
	selector           bool
	banner             bool
	redirect           bool
	jsonLD             []string
}

// parsePage reads one generated page into its metadata and version markup.
func parsePage(p string) (*parsedPage, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", p, err)
	}
	pp := &parsedPage{metas: map[metaTag]string{}, metaCounts: map[metaTag]int{}}
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, inHead bool) {
		if n.Type == html.ElementNode {
			for _, a := range n.Attr {
				switch a.Key {
				case "data-version-selector":
					pp.selector = true
				case "data-version-banner":
					pp.banner = true
				}
			}
			switch n.Data {
			case "head":
				inHead = true
			case "body":
				inHead = false
			case "meta":
				if inHead {
					pp.addMeta(n)
				}
			case "link":
				if inHead {
					pp.addLink(n)
				}
			case "script":
				if attr(n, "type") == "application/ld+json" {
					pp.jsonLD = append(pp.jsonLD, textContent(n))
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inHead)
		}
	}
	walk(doc, false)
	return pp, nil
}

func (pp *parsedPage) addMeta(n *html.Node) {
	var content string
	var keys []metaTag
	for _, a := range n.Attr {
		switch a.Key {
		case "content":
			content = a.Val
		case "name", "property", "itemprop":
			keys = append(keys, metaTag{a.Key, a.Val})
		case "http-equiv":
			if strings.EqualFold(a.Val, "refresh") {
				pp.redirect = true
			}
		}
	}
	for _, key := range keys {
		pp.metaCounts[key]++
		if _, seen := pp.metas[key]; !seen {
			pp.metas[key] = content
		}
	}
}

func (pp *parsedPage) addLink(n *html.Node) {
	var rel, typ, href, hreflang string
	for _, a := range n.Attr {
		switch a.Key {
		case "rel":
			rel = a.Val
		case "type":
			typ = a.Val
		case "href":
			href = a.Val
		case "hreflang":
			hreflang = a.Val
		}
	}
	if hasToken(rel, "canonical") {
		if pp.hasCanonical {
			pp.duplicateCanonical = true
		} else {
			pp.canonical = href
			pp.hasCanonical = true
		}
	}
	if !hasToken(rel, "alternate") {
		return
	}
	if typ == "text/markdown" && !pp.hasMDAlt {
		pp.mdAlt = href
		pp.hasMDAlt = true
	}
	if hreflang != "" {
		pp.hreflangs = append(pp.hreflangs, hreflang)
	}
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func hasToken(list, want string) bool {
	for _, token := range strings.Fields(list) {
		if token == want {
			return true
		}
	}
	return false
}

func textContent(n *html.Node) string {
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.TextNode {
			b.WriteString(c.Data)
		}
	}
	return b.String()
}

// checkPage returns the problems of one page and whether it carries a
// SoftwareApplication JSON-LD object.
func checkPage(pp *parsedPage, cls class, canonicalBase, rel string) ([]string, bool) {
	if pp.redirect {
		return nil, false
	}

	var problems []string
	want, ok := pp.metas[description]
	if !ok || strings.TrimSpace(want) == "" {
		problems = append(problems, description.String()+" is missing or empty")
	}
	for _, tag := range append([]metaTag{description, ogType}, sameAsDescriptions...) {
		if v, ok := pp.metas[tag]; ok && v != strings.TrimSpace(v) {
			problems = append(problems, fmt.Sprintf("%s has surrounding whitespace: %q", tag, v))
		}
	}
	if v, ok := pp.metas[ogType]; !ok || strings.TrimSpace(v) == "" {
		problems = append(problems, ogType.String()+" is missing or empty")
	}
	for _, tag := range sameAsDescriptions {
		v, ok := pp.metas[tag]
		if ok && strings.TrimSpace(v) != strings.TrimSpace(want) {
			problems = append(problems, fmt.Sprintf("%s differs from the description: %q", tag, truncate(v)))
		}
	}

	gotRobots, hasRobots := pp.metas[robotsTag]
	if !hasRobots {
		problems = append(problems, robotsTag.String()+" is missing")
	} else if strings.TrimSpace(gotRobots) != cls.robots() {
		problems = append(problems, fmt.Sprintf("%s is %q, want %q", robotsTag, gotRobots, cls.robots()))
	}
	if pp.metaCounts[robotsTag] > 1 {
		problems = append(problems, robotsTag.String()+" appears more than once")
	}

	if !pp.hasMDAlt {
		problems = append(problems, "no text/markdown alternate link")
	}
	if !pp.hasCanonical {
		problems = append(problems, `link rel="canonical" is missing`)
	} else if want, known := expectedCanonical(pp, cls, canonicalBase, rel); known && pp.canonical != want {
		problems = append(problems, fmt.Sprintf(`link rel="canonical" is %q, want %q`, pp.canonical, want))
	}
	if pp.duplicateCanonical {
		problems = append(problems, `link rel="canonical" appears more than once`)
	}

	ogURL, hasOGURL := pp.metas[ogURLTag]
	switch {
	case !hasOGURL || strings.TrimSpace(ogURL) == "":
		problems = append(problems, ogURLTag.String()+" is missing or empty")
	case pp.hasCanonical && ogURL != pp.canonical:
		problems = append(problems, fmt.Sprintf("%s differs from the canonical: %q", ogURLTag, truncate(ogURL)))
	}

	if cls == classLatest {
		for _, want := range []string{"en", "zh-CN", "x-default"} {
			if !slices.Contains(pp.hreflangs, want) {
				problems = append(problems, fmt.Sprintf(`link rel="alternate" hreflang=%q is missing`, want))
			}
		}
	} else if len(pp.hreflangs) > 0 {
		problems = append(problems, fmt.Sprintf("hreflang links are not allowed for class %s: %s", cls, strings.Join(pp.hreflangs, ", ")))
	}

	if !pp.selector {
		problems = append(problems, "no element with data-version-selector")
	}
	if cls.banner() && !pp.banner {
		problems = append(problems, "no element with data-version-banner")
	}
	if !cls.banner() && pp.banner {
		problems = append(problems, fmt.Sprintf("element with data-version-banner is not allowed for class %s", cls))
	}

	appFound := false
	for _, raw := range pp.jsonLD {
		if !json.Valid([]byte(raw)) {
			problems = append(problems, "JSON-LD block is not valid JSON")
			continue
		}
		for _, obj := range decodeJSONLD(raw) {
			if typ, _ := obj["@type"].(string); typ != "SoftwareApplication" {
				continue
			}
			appFound = true
			version, hasVersion := obj["softwareVersion"]
			if cls == classDev {
				if hasVersion {
					problems = append(problems, "JSON-LD SoftwareApplication carries softwareVersion for class dev")
				}
				continue
			}
			s, _ := version.(string)
			if !hasVersion || strings.TrimSpace(s) == "" {
				problems = append(problems, "JSON-LD SoftwareApplication has no softwareVersion")
			}
		}
	}
	return problems, appFound
}

func expectedCanonical(pp *parsedPage, cls class, canonicalBase, rel string) (string, bool) {
	if cls == classLatestCopy {
		return canonicalBase + pageDir(rel), true
	}
	if !pp.hasMDAlt {
		return "", false
	}
	return strings.TrimSuffix(pp.mdAlt, "index.md"), true
}

func decodeJSONLD(raw string) []map[string]any {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil
	}
	switch t := v.(type) {
	case map[string]any:
		return []map[string]any{t}
	case []any:
		var objs []map[string]any
		for _, e := range t {
			if obj, ok := e.(map[string]any); ok {
				objs = append(objs, obj)
			}
		}
		return objs
	}
	return nil
}

// truncate shortens a value for a message.
func truncate(s string) string {
	const limit = 80
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "…"
}

type versionFileKind int

const (
	kindIndex versionFileKind = iota
	kindLLMS
	kindLLMSFull
)

// checkVersionFile reports every Version line missing or malformed in a
// Markdown rendition, llms.txt or llms-full.txt.
func checkVersionFile(p string, kind versionFileKind, cls class) ([]string, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	lines := splitLines(string(data))
	switch kind {
	case kindIndex:
		if i := firstSourceLine(lines); i >= 0 {
			return checkAfterSource(lines, i, cls), nil
		}
		return checkAfterSummary(lines, cls), nil
	case kindLLMS:
		return checkAfterSummary(lines, cls), nil
	case kindLLMSFull:
		var problems []string
		found := false
		for i := range lines {
			if isSourceLine(lines[i]) {
				found = true
				problems = append(problems, checkAfterSource(lines, i, cls)...)
			}
		}
		if !found {
			problems = append(problems, `no "Source:" block`)
		}
		return problems, nil
	}
	return nil, nil
}

func splitLines(s string) []string {
	return strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
}

func isSourceLine(line string) bool {
	return strings.HasPrefix(line, "Source: ")
}

func firstSourceLine(lines []string) int {
	for i, line := range lines {
		if isSourceLine(line) {
			return i
		}
	}
	return -1
}

func checkAfterSource(lines []string, i int, cls class) []string {
	next := i + 1
	if next >= len(lines) || !strings.HasPrefix(lines[next], "Version: ") {
		return []string{`the line after "Source:" is not a Version line`}
	}
	return versionFormProblems(lines[next], cls)
}

func checkAfterSummary(lines []string, cls class) []string {
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "> ") {
			start = i
			break
		}
	}
	if start < 0 {
		return []string{`no "> " summary line to place the Version line after`}
	}
	for _, line := range lines[start+1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, "Version: ") {
			return []string{`the first line after the "> " summary is not a Version line`}
		}
		return versionFormProblems(line, cls)
	}
	return []string{`no Version line after the "> " summary`}
}

func versionFormProblems(line string, cls class) []string {
	ok := false
	switch cls {
	case classLatest, classLatestCopy:
		ok = strings.HasSuffix(line, "(latest release)")
	case classOlder:
		ok = strings.Contains(line, "(superseded; latest release ")
	case classDev:
		ok = strings.HasPrefix(line, "Version: development (unreleased; latest release ")
	}
	if ok {
		return nil
	}
	return []string{fmt.Sprintf("Version line is not the %s form: %q", cls, truncate(line))}
}

// checkSitemap reports every URL of a sitemap that has no lastmod. A sitemap
// index lists other sitemaps rather than pages and is not checked.
func checkSitemap(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var set struct {
		XMLName xml.Name
		URLs    []struct {
			Loc     string `xml:"loc"`
			Lastmod string `xml:"lastmod"`
		} `xml:"url"`
	}
	if err := xml.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if set.XMLName.Local != "urlset" {
		return nil, nil
	}
	var problems []string
	for _, u := range set.URLs {
		if strings.TrimSpace(u.Lastmod) == "" {
			problems = append(problems, "no lastmod for "+u.Loc)
		}
	}
	return problems, nil
}
