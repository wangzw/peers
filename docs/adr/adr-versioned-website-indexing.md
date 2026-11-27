# Index only the website's latest release at the root

## Context

The website at <https://peers.zhanwei.wang/> serves several versions of the documentation from one host: the latest published release at `/` and `/zh/`, the same release again at its own `/vX.Y.Z/`, every other published release from v0.6.1 on at its own `/vX.Y.Z/`, and the unreleased content of main at `/dev/`. The same page therefore exists at several URLs, with identical text at the root and in the latest release's `/vX.Y.Z/` copy, and differing text between releases.

A reader who follows a search result or a generative engine's citation runs the commands the page shows. Text from an older release or from unreleased main, presented as current, gives commands and flags that do not match the release the reader installs. One version alone may be presented as current: the latest release, at the root, where the README, the issue templates and external links already point.

A canonical link serves consolidation: Google chooses one canonical URL among pages it judges to be duplicates by the similarity of their content, and counts a canonical link as a strong signal in that choice ([REF-google-consolidate-duplicate-urls](../references.md)). A `noindex` rule serves exclusion: it keeps a page out of search results once a crawler fetches the page, and a crawler reads it only on a page it may fetch, so a page disallowed in robots.txt cannot show it ([REF-google-block-indexing](../references.md)). On one page, `noindex` and a canonical link to another page ask for opposite outcomes. The latest release's `/vX.Y.Z/` copy duplicates the root; pages of an older release or of `/dev/` differ from their latest counterparts, and some have no counterpart.

Generative engines and agents read the server-rendered HTML, each page's Markdown rendition (`index.md`) and the llms.txt files ([REF-llms-txt](../references.md)), and many of them run no JavaScript. The Markdown and plain-text files carry no robots meta, and GitHub Pages, which hosts the site, cannot set response headers. Every version is rendered by one template set, so every signal follows from the version class a build is given.

## Options Considered

### Index every version, each page with a self canonical

**Rejected**: near-duplicate pages compete with each other, a search engine may select any of them, and an older or unreleased page can be ranked or cited as current.

### Canonical link from every older release and from /dev/ to the latest counterpart

**Rejected**: a canonical link asks to consolidate duplicates, while pages of different releases differ in their text and are meant to stay out of search results rather than to merge into the latest; a page with no counterpart in the latest release has no target; and combined with `noindex` it contradicts it.

### Disallow the version paths in robots.txt

**Rejected**: crawlers then cannot fetch the pages to read their `noindex`, and the URLs can still be indexed from links to them.

### Mark the version only through client-side script

**Rejected**: crawlers and agents that run no JavaScript would see no version.

### An `X-Robots-Tag: noindex` response header on the Markdown and plain-text files of older releases and /dev/

**Not adopted**: GitHub Pages cannot set response headers, so the repository cannot provide it. A rule at the proxy in front of the host can add it without displacing this decision.

### Only the root latest indexable, every version labelled on the server

**Adopted**; see Decision.

## Decision

Adopt a single indexable copy of the documentation, the latest release at the root, with every version labelled in server-rendered output. Each build belongs to one of four version classes, a closed set:

- `latest`, the build at `/`: `index, follow`, a self canonical, and hreflang alternates for each language with `x-default`. The host's sitemap and robots.txt come from this build alone.
- `latest-copy`, the latest release's build at `/vX.Y.Z/`: `index, follow`, a canonical to the same path under `/`, no hreflang and no sitemap.
- `older`, every other published release: `noindex, follow`, a self canonical, no hreflang and no sitemap.
- `dev`, the build at `/dev/`: the same signals as `older`.

In every class, `og:url` equals the canonical, no page carries `noindex` together with a canonical to another page, and robots.txt disallows no version path. Every page names its version in server-rendered output: every HTML page carries a version selector linking to the same page in each published release and in `/dev/`, and every page of `older` and `dev` carries a banner naming the latest release and linking to the page's latest counterpart. Every Markdown rendition and the llms.txt and llms-full.txt files carry a `Version:` line, which for `older` and `dev` names the latest release and the URL of the latest counterpart; the root llms.txt links the llms.txt of every other version. The home page's structured data names the release a build documents and links to it; `dev` names no release and links to the latest. Every build is checked against the class it is given: robots, canonical, `og:url`, hreflang, sitemap, selector, banner, `Version:` lines and structured data.

## Consequences

Search results and generative-engine citations point at the root, which documents the latest release. A reader on any other version sees which version it is and where its latest counterpart is, without JavaScript.

**Costs accepted:**

- Pages of older releases and of `/dev/` do not appear in search results; a reader looking for an older release's text reaches it only through the version selector or a direct link.
- At each release every root page switches to the new release's content in one deploy, so an external citation of a root page describes the new release from then on.
- The Markdown and plain-text files of older releases and of `/dev/` carry no robots rule and stay indexable by an engine that indexes plain text, unless the proxy in front of the host adds the response header.
- The latest release's `/vX.Y.Z/` copy is never listed in search results itself; links to it count for the root page.
- Language alternates are declared only at the root; on other versions the other language is reached through the language switch.
- The signals depend on the class a build is given: a build of an older release or of `/dev/` given no class renders as `latest` and is indexable, so every deployed build has to pass `scripts/site-meta` with its class.

## Invalidation Conditions

A search console reporting `/vX.Y.Z/` or `/dev/` pages as indexed, or selecting the latest release's `/vX.Y.Z/` copy as canonical over the root page, shows that the signals are not honoured. A need for an older release to be found through search, such as a maintained older release line, undoes the premise that only the latest release is current. A Markdown or plain-text file of an older release or of `/dev/` appearing in a search index, or cited by a generative engine as current documentation, calls for the `X-Robots-Tag: noindex` response header on those files at the proxy in front of the host.
