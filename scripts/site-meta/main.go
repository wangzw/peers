// Command site-meta checks the search and sharing metadata of the built
// website.
//
// Every generated index.html must carry a description, and the description,
// Open Graph, Twitter card and schema.org tags in its head must state it
// without surrounding whitespace and agree with each other, so a search
// engine, a link preview and a language model all read the same summary.
// og:type must be set without surrounding whitespace as well. A redirect page
// Hugo writes for an alias carries no metadata and is skipped. Every URL a
// sitemap lists must carry a lastmod date.
package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/net/html"
)

func main() {
	publicDir := flag.String("public", "docs/site/public", "generated website root")
	flag.Parse()

	problems, err := check(*publicDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site-meta: %v\n", err)
		os.Exit(2)
	}
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "site-meta: "+problem)
	}
	if len(problems) > 0 {
		os.Exit(1)
	}
}

// check walks the generated tree and returns one message per problem.
func check(publicDir string) ([]string, error) {
	var pages, sitemaps []string
	err := filepath.WalkDir(publicDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
		case d.Name() == "index.html":
			pages = append(pages, path)
		case d.Name() == "sitemap.xml":
			sitemaps = append(sitemaps, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, fmt.Errorf("no index.html under %s", publicDir)
	}
	if len(sitemaps) == 0 {
		return nil, fmt.Errorf("no sitemap.xml under %s", publicDir)
	}
	sort.Strings(pages)
	sort.Strings(sitemaps)

	var problems []string
	for _, path := range pages {
		rel, err := filepath.Rel(publicDir, path)
		if err != nil {
			return nil, err
		}
		found, err := checkPage(path)
		if err != nil {
			return nil, err
		}
		for _, problem := range found {
			problems = append(problems, filepath.ToSlash(rel)+": "+problem)
		}
	}
	for _, path := range sitemaps {
		rel, err := filepath.Rel(publicDir, path)
		if err != nil {
			return nil, err
		}
		found, err := checkSitemap(path)
		if err != nil {
			return nil, err
		}
		for _, problem := range found {
			problems = append(problems, filepath.ToSlash(rel)+": "+problem)
		}
	}
	return problems, nil
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
	sameAsDescriptions = []metaTag{
		{"property", "og:description"},
		{"name", "twitter:description"},
		{"itemprop", "description"},
	}
)

// checkPage reads the meta tags in the head of one generated page.
func checkPage(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	metas := map[metaTag]string{}
	redirect := false
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, inHead bool) {
		if n.Type == html.ElementNode && n.Data == "body" {
			return
		}
		if n.Type == html.ElementNode && n.Data == "head" {
			inHead = true
		}
		if inHead && n.Type == html.ElementNode && n.Data == "meta" {
			var content string
			var keys []metaTag
			for _, a := range n.Attr {
				switch a.Key {
				case "content":
					content = a.Val
				case "name", "property", "itemprop":
					keys = append(keys, metaTag{a.Key, a.Val})
				case "http-equiv":
					redirect = redirect || strings.EqualFold(a.Val, "refresh")
				}
			}
			for _, key := range keys {
				if _, seen := metas[key]; !seen {
					metas[key] = content
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inHead)
		}
	}
	walk(doc, false)
	if redirect {
		return nil, nil
	}

	var problems []string
	want, ok := metas[description]
	if !ok || strings.TrimSpace(want) == "" {
		problems = append(problems, description.String()+" is missing or empty")
	}
	for _, tag := range append([]metaTag{description, ogType}, sameAsDescriptions...) {
		if v, ok := metas[tag]; ok && v != strings.TrimSpace(v) {
			problems = append(problems, fmt.Sprintf("%s has surrounding whitespace: %q", tag, v))
		}
	}
	if v, ok := metas[ogType]; !ok || strings.TrimSpace(v) == "" {
		problems = append(problems, ogType.String()+" is missing or empty")
	}
	for _, tag := range sameAsDescriptions {
		v, ok := metas[tag]
		if ok && strings.TrimSpace(v) != strings.TrimSpace(want) {
			problems = append(problems, fmt.Sprintf("%s differs from the description: %q", tag, truncate(v)))
		}
	}
	return problems, nil
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
